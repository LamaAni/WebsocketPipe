using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WebSocketSharp;

namespace WebsocketPipe
{
    /// <summary>
    /// Implements a binary information pipe triggered by a websocket, 
    /// The pipe will use various methods for data transfer.
    /// NOTE: currently implemented only MappedMemoryFile and Websocket.
    /// </summary>
    public class WebsocketPipe<TMessage> : IDisposable
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
                DataSocket = new WebsocketPipeMemoryMappedFileDataSocket();
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
            IWebsocketPipeDataSocket dataSocket,
            IWebsocketPipeDataSerializer<TMessage> serializer = null)
        {
            if (serializer == null)
                serializer = new WebsocketPipeBinaryFormatingDataSerializer<TMessage>();

            if (dataSocket == null)
                dataSocket = new WebsocketPipeMSGInternalDataSocket();

            Address = address;
            Serializer = serializer;
            DataSocket = dataSocket;
            PipeID = Guid.NewGuid().ToString();

            LogMethod = (d, s) => { };
        }

        #endregion

        #region General Properties

        /// <summary>
        /// Random generated id to allow for multiple mmf on the same connection name.
        /// </summary>
        public string PipeID { get; private set; } = "";

        /// <summary>
        /// The serializer used to sertialize the data to be transferred.
        /// </summary>
        public IWebsocketPipeDataSerializer<TMessage> Serializer { get; set; }

        /// <summary>
        /// The data socekt to use when sending data. If null, data is sent with the websocket request
        /// itself.
        /// </summary>
        public IWebsocketPipeDataSocket DataSocket { get; set; }

        /// <summary>
        /// The total number of messages recived.
        /// </summary>
        public long TotalMessageRecivedEvents { get; private set; }


        public const String SendAsClientWebsocketID = "_as_client";

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

        /// <summary>
        /// Method to be called on log.
        /// </summary>
        public Action<LogData,string> LogMethod { get; set; }

        TimeSpan? m_WaitTime = null;

        public TimeSpan WaitTime
        {
            get
            {
                if (m_WaitTime == null)
                    return TimeSpan.MinValue;
                return m_WaitTime.Value;
            }
            set
            {
                if (WS != null)
                    WS.WaitTime = value;
                else if (WSServer != null)
                    WSServer.WaitTime = value;
                m_WaitTime = value;
            }
        }

        #endregion

        #region Responce collections

        internal class ResponseWaitHandle: EventWaitHandle
        {
            public ResponseWaitHandle()
                : base(false, EventResetMode.ManualReset)
            {
            }

            public TMessage Response { get; internal set; } = null;
        }

        internal Dictionary<string, ResponseWaitHandle> PendingResponseWaitHandles { get; private set; }
            = new Dictionary<string, ResponseWaitHandle>();

        #endregion

        #region connection methods

        /// <summary>
        /// Implements the connection to the server.
        /// </summary>
        class WebsocketConnection : WebSocketSharp.Server.WebSocketBehavior
        {
            public WebsocketConnection(WebsocketPipe<TMessage> pipe)
            {
                Pipe = pipe;
            }

            public WebsocketPipe<TMessage> Pipe { get; private set; }

            protected override void OnClose(CloseEventArgs e)
            {
                Pipe.OnClose(e, this.ID);
                base.OnClose(e);
            }

            protected override void OnError(WebSocketSharp.ErrorEventArgs e)
            {
                Pipe.OnError(e, this.ID);
                base.OnError(e);
            }

            protected override void OnMessage(WebSocketSharp.MessageEventArgs e)
            {
                Pipe.OnDataRecived(e, this.ID);
                base.OnMessage(e);
            }

            protected override void OnOpen()
            {
                Pipe.OnOpen(this.ID);
                base.OnOpen();
            }
        }

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
            WSServer.AddWebSocketService<WebsocketConnection>(
                address.AbsolutePath, () => new WebsocketConnection(this));

            WSServer.Log.Output = (d, s) => LogMethod(d, s);

            if (m_WaitTime == null)
                m_WaitTime = WSServer.WaitTime;
            else WSServer.WaitTime = WaitTime;
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
            WS.OnClose += (s, e) => OnClose(e, SendAsClientWebsocketID);
            WS.OnOpen += (s, e) =>OnOpen(SendAsClientWebsocketID);
            WS.OnError+= (s, e) => OnError(e, SendAsClientWebsocketID);
            WS.OnMessage+= (s, e) => OnDataRecived(e, SendAsClientWebsocketID);

            WS.Log.Output = (d, s) =>LogMethod(d, s);

            if (m_WaitTime == null)
                m_WaitTime = WS.WaitTime;
            else WS.WaitTime = WaitTime;
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
            
            DataSocket.Initialize();
        }

        /// <summary>
        /// Either stop listenting or disconnect from the server depending if this is a client or a server.
        /// </summary>
        public void Stop()
        {
            if (WS != null)
                Disconnect();
            if (WSServer != null)
                StopListening();
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

            DataSocket.Close();
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

            DataSocket.Initialize();
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

            DataSocket.Close();
        }

        #endregion

        #region message processing (WebsocketSharp)

        protected void OnError(WebSocketSharp.ErrorEventArgs e, string id)
        {
        }

        protected void OnClose(WebSocketSharp.CloseEventArgs e, string id)
        {
            TriggerWaitHandle(id, null);
        }

        private bool TriggerWaitHandle(string datasocketID, TMessage msg)
        {
            if (PendingResponseWaitHandles.ContainsKey(datasocketID))
            {
                PendingResponseWaitHandles[datasocketID].Response = msg;
                PendingResponseWaitHandles[datasocketID].Set();
                return true;
            }
            return false;
        }

        protected void OnDataRecived(WebSocketSharp.MessageEventArgs e, string id)
        {
            string datasocketId = MakeDataSocketID(id);

            TotalMessageRecivedEvents++;
            MemoryStream ms = new MemoryStream(e.RawData);
            var msgs = DataSocket.ReadMessages(ms);
            ms.Close();
            ms.Dispose();
            ms = null;

            foreach (var msg in msgs)
            {
                MessageEventArgs me = new MessageEventArgs(Serializer.FromBytes(msg.Data), msg.NeedsResponse, datasocketId);

                if (TriggerWaitHandle(datasocketId, me.Message)) // this message is a response.
                    continue;

                OnMessage(me);

                if(me.RequiresResponse)
                {
                    Send(me.Response, id);
                }
            }
        }

        protected void OnOpen(string id)
        {
        }

        #endregion

        #region Message processing

        protected virtual void OnMessage(MessageEventArgs e)
        {
            if (MessageRecived != null)
            {
                MessageRecived(this, e);
            }
        }

        #endregion

        #region Message sending

        /// <summary>
        /// Sends a message to the server/ all clients (if is a server).
        /// </summary>
        /// <param name="msg">The message to send</param>
        /// /// <param name="response">If not null, the thread will wait for response.</param>
        public void Send(TMessage msg, Action<TMessage> response = null)
        {
            Send(msg, (string[])null, response);
        }

        /// <summary>
        /// Sends a message to a sepcific client by the client id. Method should be used for servers.
        /// </summary>
        /// <param name="msg">The message to send</param>
        /// <param name="clientIds">The client ids to send the msg to, if a server.</param>
        /// /// <param name="response">If not null, the thread will wait for response from the client.</param>
        public void Send(TMessage msg, string clientId, Action<TMessage> response = null)
        {
            Send(msg, clientId == null ? null : new string[] { clientId }, response);
        }

        /// <summary>
        /// Sends a message to sepcific clients by the client id. Method should be used for servers.
        /// </summary>
        /// <param name="msg">The message to send</param>
        /// <param name="clientIds">The client ids to send the msg to, if a server.</param>
        /// /// <param name="response">If not null, the thread will wait for response.</param>
        public void Send(TMessage msg, string[] clientIds, Action<TMessage> response = null)
        {
            if (clientIds != null && WS != null)
                throw new Exception("You are trying to send a message to specific clients from a client WebsocketPipe. This is not A server.");

            WebsocketPipeMessageInfo minfo = new WebsocketPipeMessageInfo(Serializer.ToBytes<TMessage>(msg), null, response != null);

            if (WS != null)
            {
                minfo.DataSocketId = MakeDataSocketID(SendAsClientWebsocketID);

                if (response == null)
                {
                    // Send async and do nothing.
                    WS.SendAsync(GetWebsocketMessageData(minfo), (t) => { });
                }
                else
                {
                    // need to send and wait for response.
                    ResponseWaitHandle hndl = new ResponseWaitHandle();
                    PendingResponseWaitHandles[minfo.DataSocketId] = hndl;
                    WS.SendAsync(GetWebsocketMessageData(minfo),(t)=>
                    {
                        if(!t)
                        {
                            // not complete sending. 
                            // error. 
                            // response is null.
                            hndl.Response = null;
                            hndl.Set();
                        }
                    });

                    hndl.WaitOne();

                    // Removing the wait handle.
                    PendingResponseWaitHandles.Remove(minfo.DataSocketId);

                    // The response to the sending.
                    response(hndl.Response);
                }
            }
            else if (WSServer != null)
            {
                IEnumerable<WebSocketSharp.Server.IWebSocketSession> sessions = FindValidSession(clientIds);

                List<ResponseWaitHandle> waitHandles = response == null ? null : new List<ResponseWaitHandle>();

                // Sending to the specific clients.
                foreach (var client in sessions)
                {
                    minfo.DataSocketId = MakeDataSocketID(client.ID);

                    if (response == null)
                    {
                        // Send async and do nothing.
                        client.Context.WebSocket.SendAsync(GetWebsocketMessageData(minfo), (t) => { });
                    }
                    else
                    {
                        // Send async and wait for response.
                        ResponseWaitHandle hndl = new ResponseWaitHandle();
                        PendingResponseWaitHandles[minfo.DataSocketId] = hndl;
                        client.Context.WebSocket.SendAsync(GetWebsocketMessageData(minfo), (t) =>
                        {
                            if (!t)
                            {
                                // not complete sending. 
                                // error. 
                                // response is null.
                                hndl.Response = null;
                                hndl.Set();
                            }
                        });
                    }
                }

                if (waitHandles != null && waitHandles.Count > 0)
                {
                    // waiting
                    foreach (var hndl in waitHandles)
                        hndl.WaitOne(); // all need to complete.

                    foreach (var rmsg in waitHandles.Select(h => h.Response))
                        response(rmsg);
                }
            }
            else throw new Exception("You are not connected to any servers." +
                "Please use method Connect to connect to a server or method listen to wait for others to connecto to you.");
        }

        private string MakeDataSocketID(string id)
        {
            return PipeID + "_" + id;
        }

        private IEnumerable<WebSocketSharp.Server.IWebSocketSession> FindValidSession(string[] clientIds)
        {
            // finding clients with matching ids.
            List<WebSocketSharp.Server.IWebSocketSession> clients =
                new List<WebSocketSharp.Server.IWebSocketSession>();

            HashSet<string> clientSet = clientIds == null ? null : new HashSet<string>(clientIds);
            Func<string, bool> isClientOK = (id) =>
            {
                if (clientSet == null)
                    return true;
                return clientSet.Contains(id);
            };

            var sessions = WSServer.WebSocketServices.Hosts.SelectMany(
                host => host.Sessions.Sessions.Where(s => isClientOK(s.ID)));
            return sessions;
        }

        private void doSend(TMessage msg, Action<bool, string> asyncOnComplete, string id)
        {

        }

        private byte[] GetWebsocketMessageData(WebsocketPipeMessageInfo msg)
        {
            MemoryStream strm = new MemoryStream();
            DataSocket.WriteMessage(msg, strm);
            byte[] data = strm.ToArray();
            strm.Close();
            strm.Dispose();
            return data;
        }

        #endregion

        #region events

        /// <summary>
        /// On message event args
        /// </summary>
        public class MessageEventArgs : EventArgs
        {
            internal MessageEventArgs(TMessage msg, bool needsResponse, string wsockID)
            {
                Message = msg;
                RequiresResponse = needsResponse;
                WebsocketID = wsockID;
            }

            /// <summary>
            /// If true this message requires a response of type TMessage. Can be null.
            /// </summary>
            public bool RequiresResponse { get; private set; }

            /// <summary>
            /// The message.
            /// </summary>
            public TMessage Message { get; private set; }

            /// <summary>
            /// The response to send back if one is needed.
            /// </summary>
            public TMessage Response { get; set; }

            /// <summary>
            /// The id of the wensocket.
            /// </summary>
            public string WebsocketID { get; private set; }
        }

        /// <summary>
        /// Called when a message is recived.
        /// </summary>
        public event EventHandler<MessageEventArgs> MessageRecived;

        #endregion

        #region Dispose


        public void Dispose()
        {
            if (WS != null && WS.IsAlive)
                WS.Close();
            if (WSServer != null && WSServer.IsListening)
                WSServer.Stop();

            if (DataSocket != null)
                DataSocket.Close();

            WS = null;
            WSServer = null;
            DataSocket = null;
            Serializer = null;
        }

        #endregion
    }

}