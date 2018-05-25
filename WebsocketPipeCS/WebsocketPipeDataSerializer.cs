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
            if (msg == null)
                return new byte[0];

            MemoryStream strm = new MemoryStream();
            try
            {
                serializer.WriteTo(strm, msg);
            }
            catch (Exception ex)
            {
                throw new Exception("Serializer failed to serialize a message)", ex);
            }
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
            TMessage msg;
            try
            {
                msg = serializer.ReadFrom(strm);
            }
            catch (Exception ex)
            {
                throw new Exception("Serializer failed to deserialize a message (" + data.Length + " bytes)", ex);
            }
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
        private WebsocketPipeBinaryFormatingDataSerializerTypeBinder m_Binder;

        /// <summary>
        /// The binary formatter used when serializing.
        /// </summary>
        internal WebsocketPipeBinaryFormatingDataSerializerTypeBinder Binder
        {
            get
            {
                if (m_Binder == null)
                {
                    m_Binder = new WebsocketPipeBinaryFormatingDataSerializerTypeBinder();
                }
                return m_Binder;
            }
            set
            {
                m_Binder = value;
            }
        }

        internal class WebsocketPipeBinaryFormatingDataSerializerTypeBinder : System.Runtime.Serialization.SerializationBinder
        {
            public Dictionary<string, Type> Mapped { get; private set; } = new Dictionary<string, Type>();

            public override Type BindToType(string assemblyName, string typeName)
            {
                string bindName = assemblyName + typeName;
                if (!Mapped.ContainsKey(bindName))
                {
                    System.Reflection.Assembly dataAssembly = typeof(TMessage).Assembly;
                    Type t = dataAssembly.GetType(typeName);
                    if (t == null)
                        t = getTypeFromAssembly(typeName,System.Reflection.Assembly.GetEntryAssembly());
                    if (t == null)
                        t = getTypeFromAssembly(typeName, System.Reflection.Assembly.GetCallingAssembly());
                    if (t == null)
                        t = getTypeFromAssembly(typeName, System.Reflection.Assembly.GetExecutingAssembly());
                    if (t == null)
                        t = getTypeFromAssembly(typeName, GetAssemblyByName(assemblyName));
                    if (t == null)
                        throw new Exception("Cannot find type with typename " + typeName + " that orinated in assembly " + assemblyName);
                    Mapped[bindName] = t;
                }
                return Mapped[bindName];
            }
        }

        static System.Reflection.Assembly GetAssemblyByName(string name)
        {
            return AppDomain.CurrentDomain.GetAssemblies().
                   SingleOrDefault(assembly => assembly.GetName().Name == name);
        }

        static Type getTypeFromAssembly(string typeName, System.Reflection.Assembly asm)
        {
            if (asm == null)
                return null;
            return asm.GetType(typeName);
        }

        private BinaryFormatter CreateBinaryFormatter()
        {
            BinaryFormatter bf = new BinaryFormatter();

            bf.TypeFormat = System.Runtime.Serialization.Formatters.FormatterTypeStyle.TypesWhenNeeded;
            bf.AssemblyFormat = System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple;
            bf.Binder = Binder;
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
            return CreateBinaryFormatter().Deserialize(strm) as TMessage;
        }

        /// <summary>
        /// Writes a message to the stream.
        /// </summary>
        /// <param name="strm">The stream to write to</param>
        /// <param name="msg">The message</param>
        public void WriteTo(Stream strm, TMessage msg)
        {
            CreateBinaryFormatter().Serialize(strm, msg);
        }
    }
}
