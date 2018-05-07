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
        public int MaxMemoryMappedFileCapacity = int.MaxValue;

        /// <summary>
        /// The minimal size of the memory mapped file.
        /// </summary>
        public int MinMemoryMappedFileCapacity = (int)Math.Pow(2, 12);

        /// <summary>
        /// The header size of the mmf file.
        /// </summary>
        protected int MMFHeaderSize { get; private set; } = 5;

        /// <summary>
        /// If below this msg size, the message will be send with the websocket packet. It will be just faster.
        /// </summary>
        public int UseInternalPacketDataSendingIfMsgByteSizeIsLessThen { get; set; } = 5000;

        /// <summary>
        /// The total number of memory mapped files created.
        /// </summary>
        public int TotalNumberOfMemoryMappedFilesCreated { get; private set; } = 0;

        /// <summary>
        /// The total active memory mapped files count.
        /// </summary>
        public int TotalActiveMemoryMappedFiles { get { return MemoryMapByID.Count; } }

        #endregion

        #region Memory file managment

        /// <summary>
        /// A collection of memory mapped files to be used for data transfer.
        /// </summary>
        protected Dictionary<string, Tuple<int, MemoryMappedFile>> MemoryMapByID { get; private set; } =
            new Dictionary<string, Tuple<int, MemoryMappedFile>>();

        static int nextPowerOf2(int v)
        {
            v--;
            for (int i = 1; i < 64; i *= 2)
                v |= v >> i;
            v++;
            return v;
        }


        int ToMMFFileSize(int size)
        {
            if (size < MinMemoryMappedFileCapacity)
                return MinMemoryMappedFileCapacity;

            return nextPowerOf2(size);
        }

        protected MemoryMappedViewStream GetDataWritingMemoryMappedViewStream(string id, ref int totalDataSize)
        {
            bool needAppend = false;
            bool needNew = true;

            MemoryMappedViewStream strm = null;
            if (MemoryMapByID.ContainsKey(id))
            {
                // Check the size. If smaller then need to increase.
                MemoryMappedFile mmf = MemoryMapByID[id].Item2;
                strm = mmf.CreateViewStream(0, 0, MemoryMappedFileAccess.ReadWrite);
                strm.Seek(0, SeekOrigin.Begin);

                needAppend = strm.ReadByte() == 0;

                // need to update the data size.
                if(needAppend)
                    totalDataSize = totalDataSize + MemoryMapByID[id].Item1;

                // we need a new file if the data required 
                needNew = totalDataSize > ToMMFFileSize(MemoryMapByID[id].Item1);
            }

            byte[] oldData = null;
            if(needAppend && needNew)
            {
                // the old data to copy
                oldData = new byte[MemoryMapByID[id].Item1];
                strm.Seek(MMFHeaderSize, SeekOrigin.Begin);
                strm.Read(oldData, 0, oldData.Length);
            }
            else if(needAppend)
            {
                strm.Seek(MemoryMapByID[id].Item1 + MMFHeaderSize, SeekOrigin.Begin);
            }

            BinaryWriter wr;
            if(needNew)
            {
                // destorying the old.
                if (needAppend)
                {
                    strm.Close();
                    strm.Dispose();
                    MemoryMapByID[id].Item2.Dispose();
                }

                TotalNumberOfMemoryMappedFilesCreated++;
                MemoryMappedFile mmf = MemoryMappedFile.CreateOrOpen(id, ToMMFFileSize(totalDataSize) + MMFHeaderSize);
                strm = mmf.CreateViewStream(0, 0, MemoryMappedFileAccess.ReadWrite);

                wr = new BinaryWriter(strm);
                MemoryMapByID[id] = new Tuple<int, MemoryMappedFile>(totalDataSize, mmf);
                mmf = null;

                wr.Write((byte)0);
                wr.Write(totalDataSize);
                if(needAppend)
                    wr.Write(oldData);
                // at write position for new data.
            }
            else
            {
                wr = new BinaryWriter(strm);
                strm.Seek(0, SeekOrigin.Begin);
                wr.Write((byte)0);
                wr.Write(totalDataSize);
                if (needAppend)
                    wr.Seek(MemoryMapByID[id].Item1 + MMFHeaderSize, SeekOrigin.Begin);
                // at write position.
            }

            wr = null;
            oldData = null;
            return strm;
        }

        private static string MakeValidMmfID(string id)
        {
            id = id.Replace("\\", "__");
            //id = "Global\\" + id;
            return id;
        }

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
        public virtual void WriteMessage(WebsocketPipe<TMessage> wp, TMessage msg, Stream to, string id)
        {
            // make the memory stream.
            MemoryStream ms = new MemoryStream();
            wp.Serializer.WriteMessage(ms, msg);
            byte[] msgBytes = ms.ToArray();
            ms.Close();
            ms.Dispose();
            ms = null;
            
            // make the id and write it to the stream.
            bool isPacketInternal = msgBytes.Length < this.UseInternalPacketDataSendingIfMsgByteSizeIsLessThen;
            BinaryWriter wr;
            to.WriteByte((byte)(isPacketInternal ? 1 : 0));

            if(isPacketInternal)
            {
                wr = new BinaryWriter(to);
                wr.Write(msgBytes);
                return;
            }
            else
            {
                id = MakeValidMmfID(id);
                byte[] mmfnamebuffer = ASCIIEncoding.ASCII.GetBytes(id);
                to.Write(mmfnamebuffer, 0, mmfnamebuffer.Length);
            }

            Mutex mu = new Mutex(false, id + "_mutex");

            if(!mu.WaitOne(MemoryMappedFileAccessTimeout))
            {
                throw new Exception("Memory mapped file access timedout.");
            }

            int totalDataSize = msgBytes.Length + 4;
            MemoryMappedViewStream strm = GetDataWritingMemoryMappedViewStream(id, ref totalDataSize);
            wr = new BinaryWriter(strm);

            // we are at the position of the write, and are ready for the write. 
            // at this point we have written 0 @ bit 0, and then the totalDataSize
            // writing the msg length.
            wr.Write(msgBytes.Length);

            // writing the msg bytes.
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
        /// mmf format: [wasread? 1 byte][datasize(int)][length(int)][msg][length(int)][msg]...
        /// If wasread=1, then ignores the read since the mmf was not written to, or was already read.
        /// If wasread=0, reads all pending messages and sets wasread to 1. 
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="from"></param>
        /// <returns></returns>
        public virtual IEnumerable<TMessage> ReadMessages(WebsocketPipe<TMessage> wp, Stream from)
        {
            // reading the memory mapped file name or the msg bytes. 
            from.Seek(0, SeekOrigin.Begin);
            bool isPacketInternal = from.ReadByte() == 1;
            byte[] buffer = new byte[from.Length - 1];
            from.Read(buffer, 0, buffer.Length);

            if (isPacketInternal)
            {
                // reding from the packet.
                return new TMessage[] { wp.Serializer.ReadMessage(new MemoryStream(buffer)) };
            }

            // reading from the memory stream.
            string id = ASCIIEncoding.ASCII.GetString(buffer, 0, buffer.Length);

            buffer = null;

            // calling the mutex to verify reading.
            Mutex mu = new Mutex(false, id + "_mutex");
            mu.WaitOne(MemoryMappedFileAccessTimeout);
            
            MemoryMappedFile mmf = MemoryMappedFile.OpenExisting(id);
            Stream strm = mmf.CreateViewStream(0, 0, MemoryMappedFileAccess.ReadWrite);
            BinaryReader reader = new BinaryReader(strm);
            strm.Seek(0, SeekOrigin.Begin);

            byte[] msgsData = null;
            int totalDataLength = 0;
            if (strm.Length > MMFHeaderSize && reader.ReadByte() == 0)
            {
                // there is something we need to read.
                // reading all the contents.
                totalDataLength = reader.ReadInt32();
                msgsData = reader.ReadBytes(totalDataLength);
            }

            strm.Flush();
            strm.Close();
            strm.Dispose();

            // clearing the newly created mmf.
            mmf.Dispose();
            mmf = null;

            // release the mutex allowing others to write.
            mu.ReleaseMutex();
            mu.Dispose();
            mu = null;

            // reading the messages.
            reader = new BinaryReader(new MemoryStream(msgsData));
            strm = reader.BaseStream;
            reader.BaseStream.Seek(0, SeekOrigin.Begin);

            List<TMessage> msgs = new List<TMessage>();

            while (strm.Position < totalDataLength)
            {
                int len = reader.ReadInt32();
                if (strm.Position + len > totalDataLength)
                    break;

                msgs.Add(wp.Serializer.ReadMessage(strm));
            }

            strm.Close();
            strm.Dispose();
            reader = null;
            strm = null;
            msgsData = null;
            return msgs;
        }

        #endregion

        public virtual void Initialize()
        {
        }

        public virtual void Close()
        {
            foreach(var kvp in MemoryMapByID)
            {
                kvp.Value.Item2.Dispose();
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
