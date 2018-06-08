using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WebsocketPipe
{
    /// <summary>
    /// Implements the basic methods for the data socket, to be used with the communicator. 
    /// </summary>
    public interface IWebsocketPipeDataSocket
    {
        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="msg"></param>
        /// <param name="to"></param>
        /// <param name="id">The id of the target to write to, in the case of multiple targets (like on a server).</param>
        void WriteMessage(WebsocketPipeMessageInfo msg, Stream to);

        /// <summary>
        /// Called to recive all messages that were sent through the data socekt.
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        IEnumerable<WebsocketPipeMessageInfo> ReadMessages(Stream from);

        /// <summary>
        /// Called to initialize.
        /// </summary>
        void Initialize();

        /// <summary>
        /// Called to close and dispose of all resources used.
        /// </summary>
        void Close(string id = null);
    }

    /// <summary>
    /// USE ONLY ON SAME MACHINE. Uses a memory mapped file and writes the data to that memory mapped file.
    /// When reading messages, the message
    /// data will be the name of the memory mapped file to open and read data from.
    /// mmf format: [wasread? 1 byte]{[length]}[msg][length][msg]...
    /// If a writer detects the data was read on the next attemp, it will write over the data.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    public class WebsocketPipeMemoryMappedFileDataSocket: IWebsocketPipeDataSocket
    {
        #region Proeprties

        /// <summary>
        /// The time we wait frot he memory mapped file to be availble for read. in milliseconds.
        /// </summary>
        public int MemoryMappedFileAccessTimeout { get; set; } = 1000;

        /// <summary>
        /// If below this msg size, the message will be send with the websocket packet. It will be just faster.
        /// </summary>
        public int SendInWebsocketPacktIfByteSizeIsLessThen { get; set; } = 0;

        /// <summary>
        /// The total active memory mapped files count.
        /// </summary>
        public int TotalActiveMemoryMappedFiles { get { return MemoryMapStacksByID.Count; } }

        /// <summary>
        /// The websocketpipe data socket to be used in the case where the data should be internal to the websocket packet.
        /// </summary>
        public WebsocketPipeMSGInternalDataSocket WebsocketAsInternalDataSocket { get; private set; }
            = new WebsocketPipeMSGInternalDataSocket();

        #endregion

        #region Memory file managment

        /// <summary>
        /// A collection of memory mapped files to be used for data transfer.
        /// </summary>
        protected Dictionary<string, MemoryMappedBinaryQueue> MemoryMapStacksByID { get; private set; } =
            new Dictionary<string, MemoryMappedBinaryQueue>();

        #endregion

        #region Read write

        /// <summary>
        /// Writes the message to a memory mapped file, where the memory mapped file name is WebsocketPipe.Address + id.
        /// mmf format: [wasread? 1 byte][datasize(int)][length(int)][msg][length(int)][msg]...
        /// If wasread=0, then writes the new message to the end of the message list and advances the number of messages +1.
        /// If wasread=1, clears the mmf and then writes the new message.
        /// </summary>
        /// <param name="wp">The calling WebsocketPipe</param>
        /// <param name="msg">The message to write.</param>
        /// <param name="to">The stream to write the mmf filename to.</param>
        /// <param name="id">The id of the targer we are writing to, since there may be many we open a mmf for each</param>
        public virtual void WriteMessage(WebsocketPipeMessageInfo msg, Stream to)
        {
            if (msg.Data.Length < this.SendInWebsocketPacktIfByteSizeIsLessThen)
            {
                // write that this is an internal message.
                to.WriteByte(1);
                WebsocketAsInternalDataSocket.WriteMessage(msg, to);
                return;
            }

            // write that this is a mmf msg.
            to.WriteByte(0);
            using (StreamWriter wr = new StreamWriter(to, ASCIIEncoding.ASCII))
            {
                // write the datasocket id to the websocket packet.
                wr.Write(msg.DataSocketId);
            }

            if (!MemoryMapStacksByID.ContainsKey(msg.DataSocketId))
                MemoryMapStacksByID[msg.DataSocketId] = new MemoryMappedBinaryQueue(msg.DataSocketId);

            MemoryMappedBinaryQueue stack = MemoryMapStacksByID[msg.DataSocketId];
            stack.Enqueue(msg.ToBytes());
        }

        /// <summary>
        /// Reads the pending messages in the memory mapped file, where the memory mapped file name is in the stream from.
        /// mmf format: [wasread? 1 byte][datasize(int)][length(int)][msg][length(int)][msg]...
        /// If wasread=1, then ignores the read since the mmf was not written to, or was already read.
        /// If wasread=0, reads all pending messages and sets wasread to 1. 
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="from"></param>
        /// <returns></returns>
        public virtual IEnumerable<WebsocketPipeMessageInfo> ReadMessages(Stream from)
        {
            // reading the memory mapped file name or the msg bytes.
            if(from.ReadByte()==1)
            {
                // internal message.
                return WebsocketAsInternalDataSocket.ReadMessages(from);
            }

            // read the id from the from stram.
            StreamReader freader = new StreamReader(from, ASCIIEncoding.ASCII);
            string id = freader.ReadToEnd();

            // the stack id.
            MemoryMappedBinaryQueue stack = new MemoryMappedBinaryQueue(id);

            // reading all the binary messages.
            byte[][] msgsData = stack.Empty().ToArray();

            WebsocketPipeMessageInfo[] msgs = new WebsocketPipeMessageInfo[msgsData.Length];

            for (int i = 0; i < msgsData.Length; i++)
            {
                try
                {
                    msgs[i] = WebsocketPipeMessageInfo.FromBytes(msgsData[i]);
                }
                catch(Exception ex)
                {
                    throw new Exception("Error while deserializing a datasocket message.", ex);
                }
            }

            stack.Dispose();
            return msgs;
        }

        #endregion

        public virtual void Initialize()
        {
        }

        public virtual void Close(string id = null)
        {
            if (id == null)
            {
                lock (MemoryMapStacksByID)
                {
                    foreach (var stack in MemoryMapStacksByID.Values.ToArray())
                    {
                        stack.Dispose();
                    }
                    MemoryMapStacksByID.Clear();
                }
            }
            else if (MemoryMapStacksByID.ContainsKey(id))
            {
                MemoryMapStacksByID[id].Dispose();
                MemoryMapStacksByID.Remove(id);
            }
        }
    }

    /// <summary>
    /// Writes the message data onto the websocket stram using the WebsocketPipe serializer. 
    /// This might result in large data sockets.
    /// </summary>
    /// <typeparam name="TMessage">The type of the message</typeparam>
    public class WebsocketPipeMSGInternalDataSocket: IWebsocketPipeDataSocket
    {
        /// <summary>
        /// Header size of the binary data.
        /// </summary>
        public const int HeaderSize = 2;

        public virtual void WriteMessage(WebsocketPipeMessageInfo msg, Stream to)
        {
            msg.WriteToStream(to);
        }

        public virtual IEnumerable<WebsocketPipeMessageInfo> ReadMessages(Stream from)
        {
            return new WebsocketPipeMessageInfo[] { WebsocketPipeMessageInfo.FromStream(from) };
        }

        public virtual void Initialize()
        {
        }

        public virtual void Close(string id = null)
        {
        }
    }
}
