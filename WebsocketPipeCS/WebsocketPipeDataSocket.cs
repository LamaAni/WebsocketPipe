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
    public interface IWebsocketPipeDataSocket<TMessage>
        where TMessage:class
    {
        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="msg"></param>
        /// <param name="to"></param>
        /// <param name="id">The id of the target to write to, in the case of multiple targets (like on a server).</param>
        void WriteMessage(WebsocketPipe<TMessage> wp, TMessage msg, Stream to, string id);

        /// <summary>
        /// Called to recive all messages that were sent through the data socekt.
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        IEnumerable<TMessage> ReadMessages(WebsocketPipe<TMessage> wp, Stream from);

        /// <summary>
        /// Called to initialize.
        /// </summary>
        void Initialize();

        /// <summary>
        /// Called to close and dispose of all resources used.
        /// </summary>
        void Close();
    }

    /// <summary>
    /// USE ONLY ON SAME MACHINE. Uses a memory mapped file and writes the data to that memory mapped file.
    /// When reading messages, the message
    /// data will be the name of the memory mapped file to open and read data from.
    /// mmf format: [wasread? 1 byte][length][msg][length][msg]...
    /// If a writer detects the data was read on the next attemp, it will write over the data.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    public class WebsocketPipeMemoryMappedFileDataSocket<TMessage> : IWebsocketPipeDataSocket<TMessage>
        where TMessage:class
    {
        #region Proeprties

        /// <summary>
        /// The time we wait frot he memory mapped file to be availble for read. in milliseconds.
        /// </summary>
        public int MemoryMappedFileAccessTimeout { get; set; } = 1000;

        /// <summary>
        /// The max capacity of the memory mapped file.
        /// </summary>
        public long MemoryMappedFileMaxCapacity = int.MaxValue;

        #endregion

        #region Memory file managment

        /// <summary>
        /// A collection of memory mapped files to be used for data transfer.
        /// </summary>
        protected Dictionary<string, MemoryMappedFile> MemoryMapByID { get; private set; } =
            new Dictionary<string, MemoryMappedFile>();


        protected void ValidateWriteToMemoryMappedFile(string id)
        {
            if (MemoryMapByID.ContainsKey(id))
                return;

            MemoryMappedFile mmf = MemoryMappedFile.CreateOrOpen(id, MemoryMappedFileMaxCapacity);
            MemoryMapByID[id] = mmf;
        }

        private static string MakeValidMmfID(string id)
        {
            id = id.Replace("\\", "__");
            id = "Global\\" + id;
            return id;
        }

        #endregion


        #region Read write

        /// <summary>
        /// Writes the message to a memory mapped file, where the memory mapped file name is WebsocketPipe.Address + id.
        /// mmf format: [wasread? 1 byte][length][msg][length][msg]...
        /// If wasread=0, then writes the new message to the end of the message list and advances the number of messages +1.
        /// If wasread=1, clears the mmf and then writes the new message.
        /// </summary>
        /// <param name="wp">The calling WebsocketPipe</param>
        /// <param name="msg">The message to write.</param>
        /// <param name="to">The stream to write the mmf filename to.</param>
        /// <param name="id">The id of the targer we are writing to, since there may be many we open a mmf for each</param>
        public virtual void WriteMessage(WebsocketPipe<TMessage> wp, TMessage msg, Stream to, string id)
        {
            // make the memory stream.
            MemoryStream ms = new MemoryStream();
            wp.Serializer.WriteMessage(ms, msg);
            byte[] msgBytes = ms.ToArray();
            ms.Close();
            ms.Dispose();

            // make the id and write it to the stream.
            id = MakeValidMmfID(id);
            byte[] mmfnamebuffer = ASCIIEncoding.ASCII.GetBytes(id);
            to.Write(mmfnamebuffer, 0, mmfnamebuffer.Length);

            ValidateWriteToMemoryMappedFile(id);

            Mutex mu = new Mutex(false, id + "_mutex");

            if(!mu.WaitOne(MemoryMappedFileAccessTimeout))
            {
                throw new Exception("Memory mapped file access timedout.");
            }

            MemoryMappedFile mmf = MemoryMapByID[id];
            MemoryMappedViewStream strm = mmf.CreateViewStream(0, 0, MemoryMappedFileAccess.ReadWrite);
            BinaryWriter wr = new BinaryWriter(strm);

            // checking if read.
            if(strm.Length>0 && // has something there.
                strm.ReadByte() == 0) // was not read
            {
                // going to the end.
                strm.Seek(0, SeekOrigin.End);
            }
            else
            {
                strm.Seek(0, SeekOrigin.Begin);
                // write was not read.
                strm.WriteByte(0);
                // clearing previous data.
                strm.SetLength(1);
            }

            // writing the msg length.
            wr.Write(msgBytes.LongLength);
            // writing the msg.
            wr.Write(msgBytes);

            wr = null;
            strm.Flush();
            strm.Close();
            strm.Dispose();

            // release the mutex.
            mu.ReleaseMutex();
        }

        /// <summary>
        /// Reads the pending messages in the memory mapped file, where the memory mapped file name is in the stream from.
        /// mmf format: [wasread? 1 byte][length][msg][length][msg]...
        /// If wasread=1, then ignores the read since the mmf was not written to, or was already read.
        /// If wasread=0, reads all pending messages and sets wasread to 1. 
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="from"></param>
        /// <returns></returns>
        public virtual IEnumerable<TMessage> ReadMessages(WebsocketPipe<TMessage> wp, Stream from)
        {
            // reading the memory mapped file name.
            byte[] buffer = new byte[from.Length];
            from.Seek(0, SeekOrigin.Begin);
            from.Read(buffer, 0, buffer.Length);
            string id = ASCIIEncoding.ASCII.GetString(buffer, 0, buffer.Length);
            List<byte[]> msgs = new List<byte[]>();

            // calling the mutex to verify reading.
            Mutex mu = new Mutex(false, id + "_mutex");
            mu.WaitOne(MemoryMappedFileAccessTimeout);
            
            MemoryMappedFile mmf = MemoryMappedFile.OpenExisting(id);
            MemoryMappedViewStream strm = mmf.CreateViewStream(0, 0, MemoryMappedFileAccess.ReadWrite);
            BinaryReader reader = new BinaryReader(strm);
            strm.Seek(0, SeekOrigin.Begin);

            if(strm.Length > 1 && reader.ReadByte() ==0)
            {
                // has not been read, otherwise ignore.
                // reading till end. 
                while(strm.Position<strm.Length)
                {
                    // reading the msg legnth.
                    long len = reader.ReadInt64();
                    if(strm.Position+len>strm.Length)
                    {
                        // over the length of the file, smt went wrong or wrong format. 
                        // Breaking the procedure.
                        break;
                    }

                    byte[] msg = new byte[len];
                    long startPos = strm.Position;
                    long stopPos = startPos + len;
                    while (strm.Position < stopPos) // the reading.
                    {
                        bool readMaxInit = (strm.Position + int.MaxValue) < stopPos;
                        int toRead =  readMaxInit?
                            int.MaxValue : (int)(stopPos - strm.Position);
                        reader.Read(msg, (int)(strm.Position - startPos), toRead);
                    }

                    msgs.Add(msg);
                }

                // clear the reader.
                reader = null;

                // Writing that the stream was read. 
                strm.Seek(0, SeekOrigin.Begin);
                strm.SetLength(1);
                strm.WriteByte(1);
            }

            strm.Close();
            strm.Dispose();

            // clearing the mmf.
            mmf.Dispose();

            // release the mutex and get the messages.
            mu.ReleaseMutex();

            // reading the messages and returning
            TMessage[] found = msgs.Select(mbits =>
             {
                 return wp.Serializer.ReadMessage(new MemoryStream(mbits));
             }).ToArray();

            return found;
        }

        #endregion

        public virtual void Initialize()
        {
        }

        public virtual void Close()
        {
            foreach(var mmf in MemoryMapByID.Values)
            {
                mmf.Dispose();
            }

            MemoryMapByID.Clear();
        }
    }

    /// <summary>
    /// Writes the message data onto the websocket stram using the WebsocketPipe serializer. 
    /// This might result in large data sockets.
    /// </summary>
    /// <typeparam name="TMessage">The type of the message</typeparam>
    public class WebsocketPipeMSGInternalDataSocket<TMessage> : IWebsocketPipeDataSocket<TMessage>
        where TMessage:class
    {
        public virtual void WriteMessage(WebsocketPipe<TMessage> wp, TMessage msg, Stream to, string id)
        {
            wp.Serializer.WriteMessage(to, msg);
        }

        public virtual IEnumerable<TMessage> ReadMessages(WebsocketPipe<TMessage> wp, Stream from)
        {
            // reading the message from the websocket data.
            return new TMessage[] { wp.Serializer.ReadMessage(from) };
        }

        public virtual void Initialize()
        {
        }

        public virtual void Close()
        {
        }
    }
}
