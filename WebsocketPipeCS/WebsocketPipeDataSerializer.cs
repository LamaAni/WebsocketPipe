using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace WebsocketPipe
{
    public static class ExtentionTOIWebsocketPipeDataSerializer
    {
        public static byte[] ToBytes<TMessage>(this IWebsocketPipeDataSerializer<TMessage> serializer, TMessage msg)
            where TMessage : class
        {
            MemoryStream strm = new MemoryStream();
            serializer.WriteTo(strm, msg);
            strm.Flush();
            byte[] data = strm.ToArray();
            strm.Close();
            strm.Dispose();
            return data;
        }

        public static TMessage FromBytes<TMessage>(this IWebsocketPipeDataSerializer<TMessage> serializer, byte[] data)
             where TMessage : class
        {
            if (data.Length == 0)
                return null;

            MemoryStream strm = new MemoryStream(data);
            TMessage msg = serializer.ReadFrom(strm);
            strm.Close();
            strm.Dispose();
            return msg;
        }
    }

    public interface IWebsocketPipeDataSerializer<TMessage>
        where TMessage : class
    {
        /// <summary>
        /// Reads a message from the stream, and returns the msg.
        /// </summary>
        /// <param name="strm">The stream to read from</param>
        /// <param name="mtype">the message type</param>
        /// <returns>The message</returns>
        TMessage ReadFrom(Stream strm);

        /// <summary>
        /// Writes a message to the stream.
        /// </summary>
        /// <param name="strm">The stream to write to</param>
        /// <param name="msg">The message</param>
        void WriteTo(Stream strm, TMessage msg);
    }

    public class WebsocketPipeBinaryFormatingDataSerializer<TMessage>: IWebsocketPipeDataSerializer<TMessage>
        where TMessage: class
    {
        private BinaryFormatter m_formatter;


        /// <summary>
        /// The binary formatter used when serializing.
        /// </summary>
        public BinaryFormatter Formatter
        {
            get
            {
                if (m_formatter == null)
                {
                    m_formatter= CreateBinaryFormatter();
                }
                return m_formatter;
            }
            set
            {
                m_formatter = value;
            }
        }

        internal class WebsocketPipeBinaryFormatingDataSerializerTypeBinder : System.Runtime.Serialization.SerializationBinder
        {
            public Dictionary<string, Type> Mapped { get; private set; } = new Dictionary<string, Type>();

            public override Type BindToType(string assemblyName, string typeName)
            {
                if (!Mapped.ContainsKey(typeName))
                {
                    System.Reflection.Assembly dataAssembly = typeof(TMessage).Assembly;
                    Type t = dataAssembly.GetType(typeName);
                    if (t == null)
                        t = System.Reflection.Assembly.GetEntryAssembly().GetType(typeName);
                    if (t == null)
                        t = System.Reflection.Assembly.GetCallingAssembly().GetType(typeName);
                    if (t == null)
                        t = System.Reflection.Assembly.GetExecutingAssembly().GetType(typeName);
                    Mapped[typeName] = t;
                }
                return Mapped[typeName];
            }
        }

        private BinaryFormatter CreateBinaryFormatter()
        {
            BinaryFormatter bf = new BinaryFormatter();

            bf.TypeFormat = System.Runtime.Serialization.Formatters.FormatterTypeStyle.TypesWhenNeeded;
            bf.AssemblyFormat = System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple;
            bf.Binder = new WebsocketPipeBinaryFormatingDataSerializerTypeBinder();
            return bf;
        }

        /// <summary>
        /// Reads a message from the stream, and returns the msg.
        /// </summary>
        /// <param name="strm">The stream to read from</param>
        /// <param name="mtype">the message type</param>
        /// <returns>The message</returns>
        public TMessage ReadFrom(Stream strm)
        {
            // No type conversion since
            return Formatter.Deserialize(strm) as TMessage;
        }

        /// <summary>
        /// Writes a message to the stream.
        /// </summary>
        /// <param name="strm">The stream to write to</param>
        /// <param name="msg">The message</param>
        public void WriteTo(Stream strm, TMessage msg)
        {
            if (msg != null)
                Formatter.Serialize(strm, msg);
        }
    }
}
