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
    public class MemoryMappedBinaryStack : IDisposable
    {
        /// <summary>
        /// Create a memory mapped filed data Stack. 
        /// </summary>
        /// <param name="name">The name of the memory mapped file.</param>
        /// <param name="initialSize">The Stack size.</param>
        public MemoryMappedBinaryStack(string name, int initialSize = 1024)
        {
            if (initialSize < 128)
                initialSize = 128;

            Name = name;
            m_mmf = MemoryMappedFile.CreateOrOpen(UniqueName, initialSize);
        }

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

        #endregion

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

            int fSize = GetMemoryMappedFileLength();
            int pushSize = dataValues.Sum(ar => ar.Length) + sizeof(int) * dataValues.Length;
            int stackBinaryLength = 0;
            int count = 0;
            ReadHeader(out stackBinaryLength, out count);

            if (stackBinaryLength + pushSize + HeaderSize > fSize)
                Grow(stackBinaryLength + pushSize + HeaderSize);

            using (var strm = Shared.CreateViewStream(HeaderSize + stackBinaryLength, pushSize, MemoryMappedFileAccess.ReadWrite))
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
            WriteHeader(stackBinaryLength + pushSize, count + dataValues.Length);

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

        public int GetCount()
        {
            int count, stackBinaryLengthk;
            Lock();
            ReadHeader(out stackBinaryLengthk, out count);
            UnLock();
            return count;
        }

        #endregion

        #region Operation methods

        /// <summary>
        /// Returns the size of the memory mapped file.
        /// </summary>
        /// <returns></returns>
        protected int GetMemoryMappedFileLength()
        {
            ValidateLock();
            int len = -1;

            using (var strm = Shared.CreateViewStream())
            {
                len = (int)strm.Length;
            }

            return len;
        }

        public static int HeaderSize
        {
            get { return sizeof(int) * 2; }
        }

        protected void ReadHeader(out int stackBinaryLengthk, out int count)
        {
            ValidateLock();

            using (var reader = Shared.CreateViewAccessor(0, HeaderSize))
            {
                stackBinaryLengthk = reader.ReadInt32(0);
                count = reader.ReadInt32(sizeof(int));
            }
        }

        protected void WriteHeader(int dateEndLoc, int count)
        {
            ValidateLock();

            using (var strm = Shared.CreateViewStream(0, HeaderSize, MemoryMappedFileAccess.ReadWrite))
            {
                strm.Seek(0, SeekOrigin.Begin);
                BinaryWriter wr = new BinaryWriter(strm);
                wr.Write(dateEndLoc);
                wr.Write(count);
            }
        }

        private void ValidateLock()
        {
            if (State != AccessState.Locked)
                throw new Exception("Cannot write operate on the memory mapped file without locking it first.");
        }

        /// <summary>
        /// The mutex that is used to lock the memory mapped Stack.
        /// </summary>
        private Mutex QLock;

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
        public static string ToValidUnqiueID(string id)
        {
            id = id.Replace(" ", "_"); // no spaces.
            foreach (char c in System.IO.Path.GetInvalidFileNameChars())
            {
                id = id.Replace(c, '_');
            }
            return id;
        }
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
        /// Grows the size of the memory mapped file to size, by creating new memory mapped file.
        /// All previous data is copied. (Heavy operation).
        /// </summary>
        /// <param name="size">The new size</param>
        /// <param name="toNextMultiplyOfTwo">if true, will ceil the size to the next multiple of 2.</param>
        protected void Grow(int size, bool toNextMultipleOfTwo = true)
        {
            ValidateLock();

            size = toNextMultipleOfTwo ? NextPowerOf2(size) : size;

            int stackBinaryLength = 0;
            int count = 0;
            int fSize = GetMemoryMappedFileLength();
            ReadHeader(out stackBinaryLength, out count);

            // no need to grow.
            if (fSize >= size)
            {
                // nothing to do.
                return;
            }

            // reading the size.
            byte[] curData = new byte[stackBinaryLength];
            using (var strm = Shared.CreateViewStream(0, HeaderSize + stackBinaryLength))
            {
                BinaryReader reader = new BinaryReader(strm);
                strm.Seek(0, SeekOrigin.Begin);
                curData = reader.ReadBytes(HeaderSize + stackBinaryLength);
            }

            // create the new memory mapped file.
            m_mmf.Dispose();
            m_mmf = MemoryMappedFile.CreateOrOpen(UniqueName, size);

            // copy the old data.
            using (var strm = Shared.CreateViewStream(0, HeaderSize + stackBinaryLength))
            {
                BinaryWriter wr = new BinaryWriter(strm);
                strm.Seek(0, SeekOrigin.Begin);
                wr.Write(curData);
            }
        }


        #endregion

        #region Dispose

        public void Dispose()
        {
            if(m_mmf!=null)
            {
                m_mmf.Dispose();
            }
        }

        #endregion
    }
}
