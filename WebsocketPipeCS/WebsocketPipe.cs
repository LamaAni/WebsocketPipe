using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WebSocketSharp;

namespace WebsocketPipe
{
    /// <summary>
    /// Implements a binary information pipe triggered by a websocket, 
    /// The pipe will use various methods for data transfer.
    /// NOTE: currently implemented only MappedMemoryFile and Websocket.
    /// </summary>
    public class WebsocketPipe<TMessage> : WebSocketSharp.Server.WebSocketBehavior
        where TMessage:class
    {
        #region construction
        /// <summary>
        /// Creates a new websocket pipe, that may connect or listen to the specified address.
        /// When lisenting, please remember that the port will be blocked for communications.
        /// </summary>
        /// <param name="serializer">The serialziation agent to use when sending information. 
        /// If null creates a new WebsocketPipeBinaryFormatingDataSerializer</param>
        /// <param name="autoDetectDataSocketMethod">If true, then auto detects the baset method
        /// to use when creating the data socket. If localhost then uses the 
        /// WebsocketPipeMemoryMappedFileDataSocket which transferrs data using local memory. Otherwise data socket
        /// will be WebsocketPipeMSGInternalDataSocket, thus sending data with the websocket message itself.</param>
        public WebsocketPipe(Uri address,
            IWebsocketPipeDataSerializer<TMessage> serializer = null,
            bool autoDetectDataSocketMethod = true)
            : this(address, null, serializer)
        {
            if (autoDetectDataSocketMethod && address.IsLoopback)
                DataSocket = new WebsocketPipeMemoryMappedFileDataSocket<TMessage>();
        }

        /// <summary>
        /// Creates a new websocket pipe, that may connect or listen to the specified address.
        /// When lisenting, please remember that the port will be blocked for communications.
        /// </summary>
        /// <param name="serializer">The serialziation agent to use when sending information. 
        /// If null creates a new WebsocketPipeBinaryFormatingDataSerializer</param>
        /// <param name="dataSocket">The data socket to use when sending information, if null then information will
        /// be sent with the wesocket itself (by creating a WebsocketPipeMSGInternalDataSocket).</param>
        public WebsocketPipe(Uri address,
            IWebsocketPipeDataSocket<TMessage> dataSocket = null,
            IWebsocketPipeDataSerializer<TMessage> serializer = null)
        {
            if (serializer == null)
                serializer = new WebsocketPipeBinaryFormatingDataSerializer<TMessage>();

            if (dataSocket == null)
                dataSocket = new WebsocketPipeMSGInternalDataSocket<TMessage>();

            Address = address;
            Serializer = serializer;
            DataSocket = dataSocket;
        }

        #endregion

        #region General Properties

        /// <summary>
        /// The serializer used to sertialize the data to be transferred.
        /// </summary>
        public IWebsocketPipeDataSerializer<TMessage> Serializer { get; set; }

        /// <summary>
        /// The data socekt to use when sending data. If null, data is sent with the websocket request
        /// itself.
        /// </summary>
        public IWebsocketPipeDataSocket<TMessage> DataSocket { get; set; }

        #endregion

        #region Connection properties

        /// <summary>
        /// The address to send to.
        /// </summary>
        public Uri Address { get; private set; }

        /// <summary>
        /// The websocket client associted with this object.
        /// </summary>
        public WebSocketSharp.WebSocket WS { get; private set; }

        /// <summary>
        /// The websocket server associated with this object.
        /// </summary>
        public WebSocketSharp.Server.WebSocketServer WSServer { get; private set; }

        #endregion

        #region connection methods

        /// <summary>
        /// Creates the server. Will override old servers.
        /// </summary>
        /// <param name="address">If != null then use this addres (changes the address of the websocket pipe)</param>
        public void MakeServer(Uri address = null)
        {
            if (address == null)
                address = Address;

            // Stop the old if any.
            if (WSServer != null && WSServer.IsListening)
                WSServer.Stop();

            string serverURL = address.Scheme + "://" + address.Host + ":" + Address.Port;
            WSServer = new WebSocketSharp.Server.WebSocketServer(serverURL);

            // Creates a server.
            var self = this;
            WSServer.AddWebSocketService<WebsocketPipe<TMessage>>(
                address.AbsolutePath, () => self);
        }

        /// <summary>
        /// Creates the client. Will override old clients.
        /// </summary>
        /// <param name="address">If != null then use this addres (changes the address of the websocket pipe)</param>
        public void MakeClient(Uri address = null)
        {
            if (address == null)
                address = Address;

            if(WS!=null && WS.IsAlive)
            {
                WS.CloseAsync();
            }

            WS = new WebSocket(address.ToString());
            var self = this;
            WS.OnClose += (s, e) => self.OnClose(e);
            WS.OnOpen += (s, e) => self.OnOpen();
            WS.OnError+= (s, e) => self.OnError(e);
            WS.OnMessage+= (s, e) => self.OnMessage(e);
        }

        /// <summary>
        /// Listens (and creates a server if needed) to remote connections.
        /// </summary>
        public void Listen()
        {

            if (WS != null)
                throw new Exception("Cannot both be a server and a client." +
                    " Please use Connect if you are connecting to a server or Listen to create a server.");

            if (WSServer == null)
                MakeServer();

            WSServer.Start();
        }

        /// <summary>
        /// Stops listening to remote connections.
        /// </summary>
        public void StopListening()
        {
            if (WS != null)
                throw new Exception("Cannot both be a server and a client." +
                    " Please use Disconnect if you are diconnecting from a server or StopListening to stop the server.");

            if (WSServer == null)
                return;

            if (WSServer.IsListening)
                WSServer.Stop();
        }

        /// <summary>
        /// Connects to the remote server as a client.
        /// </summary>
        /// <param name="address"></param>
        public void Connect(bool async = false)
        {
            if (WSServer != null)
                throw new Exception("Cannot both be a server and a client." +
                    " Please use Connect if you are connecting to a server or Listen to create a server.");

            MakeClient();

            if (async)
                WS.ConnectAsync();
            else
                WS.Connect();
        }

        /// <summary>
        /// Disconnect from the server.
        /// </summary>
        /// <param name="async">If true then async</param>
        public void Disconnect(bool async = false)
        {
            if (WSServer != null)
                throw new Exception("Cannot both be a server and a client." +
                    " Please use Disconnect if you are diconnecting from a server or StopListening to stop the server.");

            if (WS == null)
                return;
            if (!WS.IsAlive)
                return;

            if (async)
                WS.CloseAsync();
            else WS.Close();
        }

        #endregion

        #region message processing (WebsocketSharp)

        protected override void OnError(ErrorEventArgs e)
        {
            base.OnError(e);
        }

        protected override void OnClose(CloseEventArgs e)
        {
            base.OnClose(e);
        }

        protected override void OnMessage(MessageEventArgs e)
        {
            base.OnMessage(e);
        }

        protected override void OnOpen()
        {
            base.OnOpen();
        }

        #endregion

        #region Message sending

        /// <summary>
        /// Sends a message to the server/ all clients (if is a server).
        /// </summary>
        /// <param name="msg">The message to send</param>
        /// /// <param name="asyncOnComplete">The action to take if async. If null, then synchronius</param>
        public void Send(TMessage msg, Action asyncOnComplete = null)
        {
            Send(msg, (string[])null, asyncOnComplete);
        }

        /// <summary>
        /// Sends a message to a sepcific client by the client id. Method should be used for servers.
        /// </summary>
        /// <param name="msg">The message to send</param>
        /// <param name="clientIds">The client ids to send the msg to, if a server.</param>
        /// <param name="asyncOnComplete">The action to take if async. If null, then synchronius</param>
        public void Send(TMessage msg, string clientId = null, Action asyncOnComplete = null)
        {
            Send(msg, clientId == null ? null : new string[] { clientId }, asyncOnComplete);
        }

        /// <summary>
        /// Sends a message to sepcific clients by the client id. Method should be used for servers.
        /// </summary>
        /// <param name="msg">The message to send</param>
        /// <param name="clientIds">The client ids to send the msg to, if a server.</param>
        /// <param name="asyncOnComplete">The action to take if async. If null, then synchronius</param>
        public void Send(TMessage msg, string[] clientIds = null, Action asyncOnComplete = null)
        {
            if (clientIds != null && WS != null)
                throw new Exception("You are trying to send a message to specific clients from a client WebsocketPipe. This is not A server.");

            byte[] msgBytes = null;

            if (WS != null)
            {
            }
            else if (WSServer != null)
            {
                if (clientIds == null)
                {
                    if (asyncOnComplete!=null)
                        WSServer.WebSocketServices.BroadcastAsync(msgBytes, asyncOnComplete);
                    else
                        WSServer.WebSocketServices.Broadcast(msgBytes);
                }
                else
                {
                    // finding clients with matching ids.
                    List<WebSocketSharp.Server.IWebSocketSession> clients =
                        new List<WebSocketSharp.Server.IWebSocketSession>();
                    HashSet<string> clientSet = new HashSet<string>(clientIds);

                    var sessions = WSServer.WebSocketServices.Hosts.SelectMany(
                        host => host.Sessions.Sessions.Where(s => clientSet.Contains(s.ID)));

                    foreach (var client in sessions)
                    {
                        if (asyncOnComplete != null)
                            client.Context.WebSocket.SendAsync(msgBytes, (t) => { if (t) asyncOnComplete(); });
                        else client.Context.WebSocket.Send(msgBytes);
                    }
                }
            }
            else throw new Exception("You are not connected to any servers." +
                "Please use method Connect to connect to a server or method listen to wait for others to connecto to you.");
        }

        #endregion

        #region events

        #endregion
    }

    /// <summary>
    /// The message info send with the pipe packet.
    /// </summary>
    public class WebsocketPipeMessageInfo
    {
        public string[] msg;
        public byte[] Data;
    }
}