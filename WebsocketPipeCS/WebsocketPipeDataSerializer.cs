using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace WebsocketPipe
{
    public interface IWebsocketPipeDataSerializer<TMessage>
        where TMessage : class
    {
        /// <summary>
        /// Reads a message from the stream, and returns the msg.
        /// </summary>
        /// <param name="strm">The stream to read from</param>
        /// <param name="mtype">the message type</param>
        /// <returns>The message</returns>
        TMessage ReadMessage(Stream strm);

        /// <summary>
        /// Writes a message to the stream.
        /// </summary>
        /// <param name="strm">The stream to write to</param>
        /// <param name="msg">The message</param>
        void WriteMessage(Stream strm, TMessage msg);
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
                    m_formatter = new BinaryFormatter();
                return m_formatter;
            }
            set
            {
                m_formatter = value;
            }
        }

        /// <summary>
        /// Reads a message from the stream, and returns the msg.
        /// </summary>
        /// <param name="strm">The stream to read from</param>
        /// <param name="mtype">the message type</param>
        /// <returns>The message</returns>
        public TMessage ReadMessage(Stream strm)
        {
            // No type conversion since
            return Formatter.Deserialize(strm) as TMessage;
        }

        /// <summary>
        /// Writes a message to the stream.
        /// </summary>
        /// <param name="strm">The stream to write to</param>
        /// <param name="msg">The message</param>
        public void WriteMessage(Stream strm, TMessage msg)
        {
            Formatter.Serialize(strm, msg);
        }
    }
}
