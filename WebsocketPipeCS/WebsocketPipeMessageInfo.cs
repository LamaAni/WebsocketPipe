using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebsocketPipe
{
    /// <summary>
    /// The message info to allow the message to be sent.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    public class WebsocketPipeMessageInfo
    {
        /// <summary>
        /// Creates a new message info.
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="senderID"></param>
        /// <param name="requiresResponse"></param>
        public WebsocketPipeMessageInfo(byte[] msg, string senderID, bool requiresResponse = false)
        {
            Data = msg;
            DataSocketId = senderID;
            RequiresResponse = requiresResponse;
        }

        /// <summary>
        /// Reads a new message info from stream.
        /// </summary>
        /// <param name="from"></param>
        /// <returns></returns>
        public static WebsocketPipeMessageInfo FromStream(Stream from)
        {
            return FromStream(new BinaryReader(from));
        }

        /// <summary>
        /// Reads a new message info from stream.
        /// </summary>
        /// <param name="from"></param>
        /// <returns></returns>
        public static WebsocketPipeMessageInfo FromStream(BinaryReader from)
        {
            bool needsResponse = from.ReadByte() == 1;
            int blen = from.ReadInt32();
            return new WebsocketPipeMessageInfo(from.ReadBytes(blen), null, needsResponse);
        }
        /// <summary>
        /// Writes the message info to stream
        /// </summary>
        /// <param name="to"></param>
        public void WriteToStream(Stream to)
        {
            WriteToStream(new BinaryWriter(to));
        }

        /// <summary>
        /// Writes the message info to stream
        /// </summary>
        /// <param name="to"></param>
        public void WriteToStream(BinaryWriter to)
        {
            to.Write((byte)(RequiresResponse ? 1 : 0));
            if (Data == null)
            {
                to.Write(0);
                return;
            }
            to.Write(Data.Length);
            to.Write(Data);
        }

        /// <summary>
        /// The message bytes to be sent.
        /// </summary>
        public byte[] Data;

        /// <summary>
        /// If true then the service will wait for response before continuing (sync sending!).
        /// </summary>
        public bool RequiresResponse = false;

        /// <summary>
        /// The Id of the sending entity.
        /// </summary>
        public string DataSocketId = null;

    }
}
