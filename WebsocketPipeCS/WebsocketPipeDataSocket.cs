using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        void SendData(WebsocketPipe<TMessage> wp, TMessage msg);

        /// <summary>
        /// Called to recive all messages that were sent through the data socekt.
        /// </summary>
        /// <param name="wp"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        IEnumerable<TMessage> GetMessages(WebsocketPipe<TMessage> wp, TMessage msg);

        /// <summary>
        /// Called to initialize.
        /// </summary>
        void Initialize();

        /// <summary>
        /// Called to close and dispose of all resources used.
        /// </summary>
        void Close();
    }

    public class WebsocketPipeMemoryMappedFileDataSocket<TMessage> : IWebsocketPipeDataSocket<TMessage>
        where TMessage:class
    {
    }

    public class WebsocketPipeMSGInternalDataSocket<TMessage> : IWebsocketPipeDataSocket<TMessage>
        where TMessage:class
    {
    }
}
