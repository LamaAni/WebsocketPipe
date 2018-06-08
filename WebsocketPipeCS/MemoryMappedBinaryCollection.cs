using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading;

namespace WebsocketPipe
{
    /// <summary>
    /// Implements a memory mapped file binary data Stack, that allows for an 
    /// arbitrary size data Stack. 
    /// </summary>
    public class MemoryMappedBinaryStack : MemoryMappedBinaryCollection
    {
        /// <summary>
        /// Create a memory mapped filed data Stack. 
        /// </summary>
        /// <param name="name">The name of the memory mapped file.</param>
        /// <param name="initalizeSizeInBytes">The Stack size.</param>
        public MemoryMappedBinaryStack(string name, int initalizeSizeInBytes = 1024)
            :base(name, initalizeSizeInBytes)
        {
        }

        protected override void InitNew()
        {
            Lock();
            WriteHeader(0, 0);
            UnLock();
        }

        #region Stack methods

        /// <summary>
        /// Set a new element to the top of the stack.
        /// </summary>
        /// <param name="data">The data block.</param>
        public void Push(byte[] data)
        {
            Push(new[] { data });
        }

        /// <summary>
        /// Set a new element to the top of the stack.
        /// </summary>
        /// <param name="data">The data block.</param>
        public void Push(byte[][] dataValues)
        {
            Lock();

            int curDataLength = 0;
            int count = 0;
            ReadHeader(out curDataLength, out count);

            int pushSize = dataValues.Sum(ar => ar.Length) + sizeof(int) * dataValues.Length;

            if (!ResizeToData(curDataLength + pushSize))
                throw new IndexOutOfRangeException("The size of the collection is smaller then the content. Please see AllowGrow.");

            using (var strm = Shared.CreateViewStream(HeaderSize + curDataLength, pushSize, MemoryMappedFileAccess.ReadWrite))
            {
                strm.Seek(0, SeekOrigin.Begin);
                BinaryWriter wr = new BinaryWriter(strm);
                foreach (var data in dataValues)
                {
                    wr.Write(data);
                    wr.Write(data.Length);
                }
            }

            // finished writing the new item, write new locations.
            WriteHeader(curDataLength + pushSize, count + dataValues.Length);

            UnLock();
        }


        /// <summary>
        /// Removes and returns one element from the top of the stak.
        /// </summary>
        /// <returns></returns>
        public byte[] Pop()
        {
            byte[] data = null;

            Lock();
            int stackBinaryLength = 0;
            int count = 0;
            ReadHeader(out stackBinaryLength, out count);

            int popSize = 0;
            using (var view = Shared.CreateViewAccessor(HeaderSize + stackBinaryLength - sizeof(int), sizeof(int)))
            {
                // reading the element size.
                popSize = view.ReadInt32(0);
            }

            using (var strm = Shared.CreateViewStream(HeaderSize + stackBinaryLength - sizeof(int) - popSize, popSize))
            {
                BinaryReader reader = new BinaryReader(strm);
                strm.Seek(0, SeekOrigin.Begin);
                data = reader.ReadBytes(popSize);
            }

            WriteHeader(stackBinaryLength - sizeof(int) - popSize, count - 1);

            UnLock();

            return data;
        }

        /// <summary>
        /// Removes and returns all the elementes in the Stack.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<byte[]> Empty()
        {
            List<byte[]> els = new List<byte[]>();
            int stackBinaryLength = 0;
            int count = 0;

            Lock();
            ReadHeader(out stackBinaryLength, out count);
            int endOffset = 0;
            using (var strm = Shared.CreateViewStream(HeaderSize, stackBinaryLength))
            {
                // reading backwards. (a little slow but what can you do).
                BinaryReader reader = new BinaryReader(strm);
                while (count > 0 && endOffset < stackBinaryLength)
                {
                    strm.Seek(stackBinaryLength - endOffset - sizeof(int), SeekOrigin.Begin);
                    int popSize = reader.ReadInt32();
                    strm.Seek(stackBinaryLength - endOffset - popSize - sizeof(int), SeekOrigin.Begin);
                    els.Add(reader.ReadBytes(popSize));
                    endOffset += popSize + sizeof(int);
                    count -= 1;
                }
            }

            // empty everything.
            WriteHeader(0, 0);

            UnLock();

            return els;
        }

        /// <summary>
        /// Returns the number of elements in the collection.
        /// </summary>
        /// <returns></returns>
        public int GetCount()
        {
            Lock();
            int count, dataLength;
            ReadHeader(out dataLength, out count);
            UnLock();
            return count;
        }

        #endregion

        #region Abstract override methods

        /// <summary>
        /// Returns the header size.
        /// </summary>
        public override int HeaderSize => sizeof(int) * 2;

        /// <summary>
        /// Reads the header information.
        /// </summary>
        /// <param name="dataLength"></param>
        /// <returns>Any other values in the header</returns>
        protected virtual void ReadHeader(out int dataLength, out int count)
        {
            count = (int)ReadHeader(out dataLength)[0];
        }

        /// <summary>
        /// Reads the header information.
        /// </summary>
        /// <param name="dataLength"></param>
        /// <returns>Any other values in the header</returns>
        protected override object[] ReadHeader(out int dataLength)
        {
            // validate the lock.
            AssertLock(); 

            // reads the header.
            object[] hinfo = new object[1];
            using (var strm = Shared.CreateViewStream(0, HeaderSize))
            {
                BinaryReader reader = new BinaryReader(strm);
                dataLength = reader.ReadInt32();
                hinfo[0] = reader.ReadInt32(); // count;
            }

            return hinfo;
        }

        /// <summary>
        /// Writes the header.
        /// </summary>
        /// <param name="dataLength">The total size of the data</param>
        /// <param name="count">The number of elements in the collection.</param>
        protected void WriteHeader(int dataLength, int count)
        {
            WriteHeader(dataLength, new object[] { count });
        }

        /// <summary>
        /// Writes the header.
        /// </summary>
        /// <param name="dataLength"></param>
        /// <param name="header"></param>
        protected override void WriteHeader(int dataLength, object[] header)
        {
            AssertLock();

            object[] hinfo = new object[1];
            using (var strm = Shared.CreateViewStream(0, HeaderSize, MemoryMappedFileAccess.ReadWrite))
            {
                BinaryWriter wr = new BinaryWriter(strm);
                wr.Write(dataLength);
                // write the count.
                wr.Write((int)header[0]);
            }
        }

        /// <summary>
        /// Reads the collection data.
        /// </summary>
        /// <returns></returns>
        protected override byte[] ReadCollectionData()
        {
            int dataLength;
            ReadHeader(out dataLength);
            // reads all the data in the collection.
            byte[] data;
            using (var strm = Shared.CreateViewStream(HeaderSize, dataLength))
            {
                BinaryReader reader = new BinaryReader(strm);
                data = reader.ReadBytes(dataLength);
            }
            return data;
        }

        /// <summary>
        /// Writes the collection data.
        /// </summary>
        /// <param name="data"></param>
        protected override void WriteCollectionData(byte[] data)
        {
            int dataLength = data.Length;
            // reads all the data in the collection.
            using (var strm = Shared.CreateViewStream(HeaderSize, dataLength, MemoryMappedFileAccess.ReadWrite))
            {
                BinaryWriter wr = new BinaryWriter(strm);
                wr.Write(data);
            }
        }

        #endregion
    }

    /// <summary>
    /// Implements a memory mapped binary queue.
    /// </summary>
    public class MemoryMappedBinaryQueue : MemoryMappedBinaryCollection
    {
        /// <summary>
        /// Create a memory mapped filed data Stack. 
        /// </summary>
        /// <param name="name">The name of the memory mapped file.</param>
        /// <param name="initalizeSizeInBytes">The Stack size.</param>
        public MemoryMappedBinaryQueue(string name, int initalizeSizeInBytes = 1024)
            : base(name, initalizeSizeInBytes)
        {
        }

        protected override void InitNew()
        {
            Lock();
            WriteHeader(0, 0, 0);
            UnLock();
        }

        #region helper classes.

        public class CircularStream : Stream
        {
            /// <summary>
            /// Implements a circular stream given a base stream.
            /// </summary>
            /// <param name="baseStream">The base stream</param>
            public CircularStream(Stream baseStream, long startPosition, bool seekToOrigin = true)
            {
                BaseStream = baseStream;

                // truncate to size.
                while (startPosition > BaseStream.Length)
                    startPosition -= BaseStream.Length;

                StartPosition = startPosition;
                if (seekToOrigin)
                    Seek(0, SeekOrigin.Begin);
            }

            /// <summary>
            /// If true the circular position will be allowed to go back over previously visited locations.
            /// </summary>
            public bool InfiniteStream { get; private set; } = true;

            /// <summary>
            /// The base stream.
            /// </summary>
            public Stream BaseStream { get; private set; }

            /// <summary>
            /// The start position for the circular stream.
            /// </summary>
            public long StartPosition { get; private set; }

            /// <summary>
            /// Flush all data in the underlining stream.
            /// </summary>
            public override void Flush()
            {
                BaseStream.Flush();
            }

            /// <summary>
            /// Returns the circular stream poistion from the base stream position/
            /// </summary>
            /// <param name="baseStreamPositon"></param>
            /// <returns></returns>
            protected virtual long ToCircularStreamPosition(long baseStreamPositon)
            {
                if (StartPosition > baseStreamPositon)
                    return BaseStream.Length - StartPosition + baseStreamPositon;
                else return baseStreamPositon - StartPosition;
            }

            /// <summary>
            /// Returns the base stream position from the circular.
            /// </summary>
            /// <param name="circularStreamPosition"></param>
            /// <returns></returns>
            protected virtual long ToBaseStreamPosition(long circularStreamPosition)
            {
                long basePos = StartPosition + circularStreamPosition;
                bool first = false;
                while (basePos > BaseStream.Length)
                {
                    if (first && !InfiniteStream)
                        throw new IndexOutOfRangeException("The position is out of range. For infinite coordinates please see InfiniteStream property.");
                    basePos = basePos - BaseStream.Length;
                }
                return basePos;
            }

            /// <summary>
            /// Seeks the stream to a position.
            /// </summary>
            /// <param name="offset">The stream offset.</param>
            /// <param name="origin">The location to seek</param>
            /// <returns></returns>
            public override long Seek(long offset, SeekOrigin origin)
            {
                return ToCircularStreamPosition(BaseStream.Seek(ToBaseStreamPosition(offset), origin));
            }

            /// <summary>
            /// Sets the length of the stream.
            /// </summary>
            /// <param name="value"></param>
            public override void SetLength(long value)
            {
                BaseStream.SetLength(value);
            }

            /// <summary>
            /// Reads a buffer from the cicular stream.
            /// </summary>
            /// <param name="buffer">The buffer to read from</param>
            /// <param name="offset">The offset in buffer to read to.</param>
            /// <param name="count">The number of bytes to read.</param>
            /// <returns>The number of bytes read.</returns>
            public override int Read(byte[] buffer, int offset, int count)
            {
                long circularStreamStart = ToCircularStreamPosition(BaseStream.Position);
                if (!InfiniteStream && count + circularStreamStart > BaseStream.Length)
                    throw new IndexOutOfRangeException("The position is out of range. For infinite coordinates please see InfiniteStream property.");

                long curPos = BaseStream.Position;
                int haveBeenRead = 0;
                int leftToRead = count;
                while (leftToRead > 0)
                {
                    // going to current.
                    BaseStream.Seek(curPos, SeekOrigin.Begin);

                    // reading block.
                    int toRead = curPos + leftToRead > BaseStream.Length ? (int)(BaseStream.Length - curPos) : leftToRead;
                    BaseStream.Read(buffer, offset + haveBeenRead, toRead);
                    haveBeenRead += toRead;
                    leftToRead = leftToRead - toRead;
                    if (leftToRead > 0)
                        curPos = 0; // circular back.
                }

                return haveBeenRead;
            }

            /// <summary>
            /// Writes a buffer to the stream
            /// </summary>
            /// <param name="buffer">The buffer to write</param>
            /// <param name="offset">The offset in buffer to read from.</param>
            /// <param name="count">The number of bytes to write.</param>
            public override void Write(byte[] buffer, int offset, int count)
            {
                long circularStreamStart = ToCircularStreamPosition(BaseStream.Position);
                if (!InfiniteStream && count + circularStreamStart > BaseStream.Length)
                    throw new IndexOutOfRangeException("The position is out of range. For infinite coordinates please see InfiniteStream property.");

                long curPos = BaseStream.Position;
                int haveBeenWritten = 0;
                int leftToWrite = count;
                while (leftToWrite > 0)
                {
                    // going to current.
                    BaseStream.Seek(curPos, SeekOrigin.Begin);

                    // reading block.
                    int toWrite = curPos + leftToWrite > BaseStream.Length ? (int)(BaseStream.Length - curPos) : leftToWrite;
                    BaseStream.Write(buffer, offset + haveBeenWritten, toWrite);
                    haveBeenWritten += toWrite;
                    leftToWrite = leftToWrite - toWrite;
                    if (leftToWrite > 0)
                        curPos = 0; // circular back.
                }
            }

            /// <summary>
            /// If true can read from stream.
            /// </summary>
            public override bool CanRead => BaseStream.CanRead;

            /// <summary>
            /// If true can seek stream.
            /// </summary>
            public override bool CanSeek => BaseStream.CanSeek;

            /// <summary>
            /// If true can write to stream.
            /// </summary>
            public override bool CanWrite => BaseStream.CanWrite;

            /// <summary>
            /// The stream length.
            /// </summary>
            public override long Length => BaseStream.Length;

            /// <summary>
            /// The current position. Set uses function Seek.
            /// </summary>
            public override long Position { get => ToCircularStreamPosition(BaseStream.Position); set => Seek(value, SeekOrigin.Begin); }
            
            /// <summary>
            /// Override the dispose.
            /// </summary>
            /// <param name="disposing"></param>
            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    BaseStream.Dispose();
                base.Dispose(disposing);
            }
        }

        #endregion

        #region Queue methods

        /// <summary>
        /// Returns the number of elements in the queue.
        /// </summary>
        /// <returns></returns>
        public int GetCount()
        {
            Lock();
            int dataLength, count, startPosition;
            ReadHeader(out dataLength, out count, out startPosition);
            UnLock();
            return count;
        }
        /// <summary>
        /// Enqueue a data batch,
        /// </summary>
        /// <param name="dataValues"></param>
        public void Enqueue(byte[] data)
        {
            Enqueue(new byte[][] { data });
        }

        /// <summary>
        /// Enqueue a data batch,
        /// </summary>
        /// <param name="dataValues"></param>
        public void Enqueue(byte[][] dataValues)
        {
            Lock();

            int curDataLength, count, startPosition;
            ReadHeader(out curDataLength, out count, out startPosition);

            // resize to new data if possible.
            int pushSize = dataValues.Sum(ar => ar.Length) + sizeof(int) * dataValues.Length;

            if (!ResizeToData(curDataLength + pushSize))
                throw new IndexOutOfRangeException("The size of the collection is smaller then the content. Please see AllowGrow.");

            // write the new items.
            using (var strm = new CircularStream(Shared.CreateViewStream(HeaderSize, GetMemoryMappedFileLength() - HeaderSize), startPosition))
            {
                // Seek to the end of the queue.
                strm.Seek(curDataLength, SeekOrigin.Begin);

                BinaryWriter wr = new BinaryWriter(strm);
                foreach (byte[] data in dataValues)
                {
                    wr.Write((int)data.Length);
                    wr.Write(data);
                }
            }

            WriteHeader(curDataLength + pushSize, count + dataValues.Length, startPosition);

            UnLock();
        }

        /// <summary>
        /// Retrives an element from the queue.
        /// </summary>
        /// <returns></returns>
        public byte[] Dequeue()
        {
            // dequeues an element.
            Lock();

            int dataLength, count, startPosition;
            ReadHeader(out dataLength, out count, out startPosition);

            // write the new items.
            byte[] data;
            using (var strm = new CircularStream(Shared.CreateViewStream(HeaderSize, GetMemoryMappedFileLength() - HeaderSize), startPosition))
            {
                BinaryReader reader = new BinaryReader(strm);
                int dlen = reader.ReadInt32();
                data = reader.ReadBytes(dlen);
            }

            int curPullLength = sizeof(int) + data.Length;
            WriteHeader(dataLength - curPullLength, count - 1, startPosition + curPullLength);

            UnLock();

            return data;
        }

        /// <summary>
        /// Empties the queue and returns the current data.
        /// </summary>
        /// <returns>The current data</returns>
        public byte[][] Empty()
        {
            Lock();

            int dataLength, count, startPosition;
            ReadHeader(out dataLength, out count, out startPosition);

            // write the new items.
            byte[][] dataValues = new byte[count][];

            using (var strm = new CircularStream(Shared.CreateViewStream(HeaderSize, GetMemoryMappedFileLength() - HeaderSize), startPosition))
            {
                BinaryReader reader = new BinaryReader(strm);
                for (int i = 0; i < count; i++)
                {
                    int dlen = reader.ReadInt32();
                    dataValues[i] = reader.ReadBytes(dlen);
                }
            }

            WriteHeader(0, 0, 0);
            WriteCollectionData(new byte[0]);

            UnLock();

            return dataValues;
        }

        #endregion

        #region Abstract override methods

        /// <summary>
        /// Returns the header size.
        /// </summary>
        public override int HeaderSize => sizeof(int) * 3;

        /// <summary>
        /// Reads the header information.
        /// </summary>
        /// <param name="dataLength"></param>
        /// <returns>Any other values in the header</returns>
        protected virtual void ReadHeader(out int dataLength, out int count, out int streamStartPosition)
        {
            object[] header = ReadHeader(out dataLength);
            count = (int)header[0];
            streamStartPosition = (int)header[1];
        }

        /// <summary>
        /// Reads the header information.
        /// </summary>
        /// <param name="dataLength"></param>
        /// <returns>Any other values in the header</returns>
        protected override object[] ReadHeader(out int dataLength)
        {
            // validate the lock.
            AssertLock();

            // reads the header.
            object[] hinfo = new object[2];
            using (var strm = Shared.CreateViewStream(0, HeaderSize))
            {
                BinaryReader reader = new BinaryReader(strm);
                dataLength = reader.ReadInt32();
                hinfo[0] = reader.ReadInt32(); // count;
                hinfo[1] = reader.ReadInt32(); // startpos;
            }

            return hinfo;
        }

        /// <summary>
        /// Writes the header.
        /// </summary>
        /// <param name="dataLength">The total size of the data</param>
        /// <param name="count">The number of elements in the collection.</param>
        protected void WriteHeader(int dataLength, int count, int streamStartPosition)
        {
            WriteHeader(dataLength, new object[] { count, streamStartPosition });
        }

        /// <summary>
        /// Writes the header.
        /// </summary>
        /// <param name="dataLength"></param>
        /// <param name="header"></param>
        protected override void WriteHeader(int dataLength, object[] header)
        {
            AssertLock();

            object[] hinfo = new object[1];
            using (var strm = Shared.CreateViewStream(0, HeaderSize, MemoryMappedFileAccess.ReadWrite))
            {
                BinaryWriter wr = new BinaryWriter(strm);
                wr.Write(dataLength);
                // write the count.
                wr.Write((int)header[0]);
                // write the stream start position.
                wr.Write((int)header[1]);
            }
        }

        /// <summary>
        /// Reads the collection data.
        /// </summary>
        /// <returns></returns>
        protected override byte[] ReadCollectionData()
        {
            AssertLock();

            int dataLength, count, startPosition;
            ReadHeader(out dataLength,out count, out startPosition);

            // reads all the data in the collection.
            byte[] data;
            using (var strm = new CircularStream(Shared.CreateViewStream(HeaderSize, GetMemoryMappedFileLength() - HeaderSize), startPosition))
            {
                BinaryReader reader = new BinaryReader(strm);
                data = reader.ReadBytes(dataLength);
            }
            return data;
        }

        /// <summary>
        /// Writes the collection data.
        /// </summary>
        /// <param name="data"></param>
        protected override void WriteCollectionData(byte[] data)
        {
            AssertLock();

            // when writing all the collection data then we can reset the stream 
            // start position to the start of the file. Then, we can rewrite the header.
            int count, startPosition, dataLength;
            ReadHeader(out dataLength, out count, out startPosition);
            dataLength = data.Length;
            startPosition = 0;
            
            // reads all the data in the collection.
            using (var strm = Shared.CreateViewStream(HeaderSize, dataLength, MemoryMappedFileAccess.ReadWrite))
            {
                BinaryWriter wr = new BinaryWriter(strm);
                wr.Write(data);
            }

            WriteHeader(dataLength, count, startPosition);
        }

        #endregion
    }

    /// <summary>
    /// Represents abstract methods for a memory mapped binary collection.
    /// </summary>
    public abstract class MemoryMappedBinaryCollection : IDisposable
    {
        /// <summary>
        /// Create a memory mapped filed data Stack. 
        /// </summary>
        /// <param name="name">The name of the memory mapped file.</param>
        /// <param name="initalizeSizeInBytes">The Stack size.</param>
        public MemoryMappedBinaryCollection(string name, int initalizeSizeInBytes = 1024)
        {
            if (initalizeSizeInBytes < 128)
                initalizeSizeInBytes = 128;
            Name = name;

            Lock();
            
            try
            {
                m_mmf = MemoryMappedFile.OpenExisting(UniqueName, MemoryMappedFileRights.ReadWrite);
                UnLock();
            }
            catch(FileNotFoundException ex)
            {
                if (ex != null)
                {
                    m_mmf = MemoryMappedFile.CreateNew(UniqueName, initalizeSizeInBytes, MemoryMappedFileAccess.ReadWrite);
                    InitNew();
                    UnLock();
                }
                else
                {
                    UnLock();
                    throw new Exception("Cannot open/create the memory mapped file.");
                }
            }
        }

        protected abstract void InitNew();

        #region helper clases

        public enum AccessState
        {
            Locked,
            UnLocked,
        }

        #endregion

        #region properties

        /// <summary>
        /// The current access state.
        /// </summary>
        public AccessState State { get; private set; } = AccessState.UnLocked;

        /// <summary>
        /// The name of the Stack. This is also the basis of the memory mapped file name.
        /// </summary>
        public string Name { get; private set; }

        string m_UniqueName = null;
        /// <summary>
        /// The name that is used for the memory mapped file.
        /// </summary>
        public string UniqueName
        {
            get
            {
                if (m_UniqueName == null)
                    m_UniqueName = ToValidUnqiueID("MemoryMappedFileDataStack_mmfuid_" + Name);
                return m_UniqueName;
            }
        }

        /// <summary>
        /// The memory mapped file associated with the Stack.
        /// </summary>
        MemoryMappedFile m_mmf = null;

        /// <summary>
        /// The memory mapped file.
        /// </summary>
        public MemoryMappedFile Shared { get { return m_mmf; } }

        /// <summary>
        /// The timeout to wait when locking the stack. [ms]
        /// </summary>
        public int LockTimeout { get; set; } = 10000;

        /// <summary>
        /// The mutex that is used to lock the memory mapped Stack.
        /// </summary>
        protected Mutex QLock { get; private set; } = null;

        /// <summary>
        /// Returns the header size.
        /// </summary>
        public abstract int HeaderSize { get; }

        /// <summary>
        /// If true the collection mmf file can grow.
        /// </summary>
        public bool AllowGrow { get; private set; } = true;

        /// <summary>
        /// If true then the collection mmf file can shrink. (Reduced performance).
        /// </summary>
        public bool AllowShrink { get; private set; } = false;

        /// <summary>
        /// If true, the shrink and grow of the collection will be in multiples of 2. Increased performance, more memory.
        /// </summary>
        public bool ResizeInMultiplesOf2 { get; private set; } = true;

        #endregion

        #region Dispose

        public void Dispose()
        {
            if (m_mmf != null)
            {
                m_mmf.Dispose();
            }
        }

        #endregion


        #region Operation methods

        /// <summary>
        /// Returns the size of the memory mapped file.
        /// </summary>
        /// <returns></returns>
        protected int GetMemoryMappedFileLength()
        {
            AssertLock();
            int len = -1;

            using (var strm = Shared.CreateViewStream())
            {
                len = (int)strm.Length;
            }

            return len;
        }

        /// <summary>
        /// Reas the collection header.
        /// </summary>
        /// <param name="dataLength">The length of the data.</param>
        /// <returns>Returns any other header values (except data length).</returns>
        protected abstract object[] ReadHeader(out int dataLengtht);

        /// <summary>
        /// Write the collection header.
        /// </summary>
        /// <param name="dataLength">The length of the data.</param>
        /// <param name="header">The header to write.</param>
        protected abstract void WriteHeader(int dataLength, object[] header);

        /// <summary>
        /// Validates the current is locked. Otherwise throws an error.
        /// </summary>
        protected void AssertLock()
        {
            if (State != AccessState.Locked)
                throw new Exception("Cannot write operate on the memory mapped file without locking it first.");
        }

        /// <summary>
        /// Reads the colection data.
        /// </summary>
        /// <param name="o"></param>
        /// <returns></returns>
        protected abstract byte[] ReadCollectionData();

        /// <summary>
        /// Writes the collection data.
        /// </summary>
        /// <param name="data">The data to write</param>
        protected abstract void WriteCollectionData(byte[] data);

        /// <summary>
        /// Opens the Stack for read and write. This will block any other Stack from accessing the same Memory mapped stack.
        /// </summary>
        protected void Lock(int timeout = -1)
        {
            if (State == AccessState.Locked)
                return;

            if (timeout < 0)
                timeout = LockTimeout;

            QLock = new Mutex(false, UniqueName + "_Mutex");
            QLock.WaitOne(timeout);
            State = AccessState.Locked;
        }

        /// <summary>
        /// Unlocks the mutex.
        /// </summary>
        protected void UnLock()
        {
            if (State == AccessState.UnLocked)
                return;

            QLock.ReleaseMutex();
            QLock = null;
            State = AccessState.UnLocked;
        }

        #endregion

        #region mmf methods

        /// <summary>
        /// Validates the id and retuns a mutex/mmf valid id.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string ToValidUnqiueID(string id)
        {
            id = id.Replace(" ", "_"); // no spaces.
            foreach (char c in System.IO.Path.GetInvalidFileNameChars())
            {
                id = id.Replace(c, '_');
            }
            return id;
        }


        #endregion

        #region Collection size.

        /// <summary>
        /// Returns the next power of two.
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        public static int NextPowerOf2(int v)
        {
            v--;
            for (int i = 1; i < 64; i *= 2)
                v |= v >> i;
            v++;
            return v;
        }

        /// <summary>
        /// Grows or Shrinks the size of the memory mapped file to size, according to AllowGrow and AllowShrink, by creating new memory mapped file.
        /// All previous data is copied. (Heavy operation).
        /// </summary>
        /// <param name="newFileSize">The new size</param>
        /// <param name="toNextMultiplyOfTwo">if true, will ceil the size to the next multiple of 2.</param>
        /// <returns>true if data can be contained in the collection.</returns>
        protected bool ResizeToData(int newDataLength)
        {
            AssertLock();

            int newFileSize = HeaderSize + (ResizeInMultiplesOf2 ? NextPowerOf2(newDataLength) : newDataLength);
            int curFileSize = GetMemoryMappedFileLength();

            int curDataLength = 0;
            object[] header = ReadHeader(out curDataLength);

            // check for growth needed.
            if (newFileSize > curFileSize && !AllowGrow)
            {
                // true if data can be contained.
                return false;
            }
            else if (newFileSize < curFileSize && !AllowShrink)
            {
                return true;
            }
            else if (curFileSize == newFileSize)
            {
                return true;
            }

            byte[] curData = ReadCollectionData();

            // create the new memory mapped file.
            m_mmf.Dispose();
            m_mmf = MemoryMappedFile.CreateOrOpen(UniqueName, newFileSize);

            WriteHeader(curDataLength, header);
            WriteCollectionData(curData);

            // resized so data can be contained.
            return true;
        }
        
        #endregion
    }
}
