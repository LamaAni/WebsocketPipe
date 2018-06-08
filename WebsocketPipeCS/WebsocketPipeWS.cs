using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WebsocketPipe.Extentions;

namespace WebsocketPipe
{
    /// <summary>
    /// Implements the websocket to be used with the websocket pipe. Can listen or connect.
    /// Might use another internal websocket.
    /// </summary>
    public class WebsocketPipeWS : IDisposable
    {
        public WebsocketPipeWS(Uri address)
        {
            Address = address;
        }

        #region propeties

        /// <summary>
        /// The ID in the case of a client.
        /// </summary>
        public string ID { get; private set; } = Guid.NewGuid().ToString();

        /// <summary>
        /// The address to connect to.
        /// </summary>
        public Uri Address { get; private set; } = null;

        /// <summary>
        /// The time to wait before sending a timeout event [ms];
        /// </summary>
        public int WaitTimeout { get; set; } = 5000;

        /// <summary>
        /// True if the websocket is alive and connected/listening.
        /// </summary>
        public bool IsAlive { get { return IsConnected || IsListening; } }

        /// <summary>
        /// THe ping interval in ms.
        /// </summary>
        public int PingInterval { get; set; } = 30000;

        #endregion

        #region as server methods

        /// <summary>
        /// Fleck websocket server if any;
        /// </summary>
        public Fleck.WebSocketServer WSServer { get; private set; }

        /// <summary>
        /// A collection of the websocket server connections.
        /// </summary>
        protected Dictionary<string, Fleck.IWebSocketConnection> ServerConnections { get; private set; } = new Dictionary<string, Fleck.IWebSocketConnection>();

        /// <summary>
        /// Returns true if connected to remote.
        /// </summary>
        public bool IsListening
        {
            get
            {
                if (WSServer == null)
                    return false;
                return WSServer.ListenerSocket != null;
            }
        }

        void CreateServer()
        {
            if (WSServer != null)
                return;
            WSServer = new Fleck.WebSocketServer(Address.ToString());
            WSServer.RestartAfterListenError = true;
        }

        /// <summary>
        /// Listen for remote connections, will block the port.
        /// </summary>
        public void Listen()
        {
            CreateServer();
            if (WSServer.ListenerSocket != null && WSServer.ListenerSocket.Connected)
                return;
            
            WSServer.Start(ConfigureServerConnection);
        }

        void ConfigureServerConnection(Fleck.IWebSocketConnection con)
        {
            // Setting the connection.
            ServerConnections[con.ConnectionInfo.Id.ToString()] = con;
            
            con.OnMessage = (msg) =>
            {
                // when reciving a string message.
                OnMessageRecived(msg, null, con.ConnectionInfo.Id.ToString());
            };

            con.OnBinary = (data) =>
            {
                // when reciving a binary message.
                OnMessageRecived(null, data, con.ConnectionInfo.Id.ToString());
            };

            con.OnError = (e) =>
            {
                // when recived an error message.
                OnErrorRecived(e, con.ConnectionInfo.Id.ToString());
            };

            con.OnClose = () =>
            {
                // when recived a close message.
                if (ServerConnections.ContainsKey(con.ConnectionInfo.Id.ToString()))
                    ServerConnections.Remove(con.ConnectionInfo.Id.ToString());
                OnClose(con.ConnectionInfo.Id.ToString());
            };

            con.OnOpen = () =>
            {
                // when recived an open message.
                OnOpen(con.ConnectionInfo.Id.ToString());
            };

            con.OnPing = (data) =>
            {
                // when recived an open message.
                OnServerPing(data,con.ConnectionInfo.Id.ToString());
                con.SendPong(data); // respond with the same.
            };

            con.OnPong = (data) =>
            {
                // do nothing here, the ping should be the same as the pong.
            };
        }

        #endregion

        #region as client methods

        /// <summary>
        /// The websocket client if any;
        /// </summary>
        public WebSocket4Net.WebSocket WSClient { get; private set; }

        /// <summary>
        /// Returns true if connected to remote.
        /// </summary>
        public bool IsConnected
        {
            get
            {
                if (WSClient == null)
                    return false;
                return WSClient.State == WebSocket4Net.WebSocketState.Open;
            }
        }

        void CreateClient()
        {
            if (WSClient != null)
                return;

            WSClient = new WebSocket4Net.WebSocket(Address.ToString());
            WSClient.AutoSendPingInterval = PingInterval / 1000;
            WSClient.EnableAutoSendPing = true;

            WSClient.Closed += (s, e) =>
             {
                 OnClose(this.ID);
             };

            WSClient.Opened += (s, e) =>
            {
                OnOpen(this.ID);
            };

            WSClient.MessageReceived += (s, e) =>
            {
                OnMessageRecived(e.Message, null, ID);
            };

            WSClient.Error += (s, e) =>
            {
                OnErrorRecived(e.Exception, ID);
            };

            WSClient.DataReceived += (s, e) =>
            {
                OnMessageRecived(null, e.Data, ID);
            };
        }

        /// <summary>
        /// Connect to a remote server.
        /// </summary>
        public void Connect(bool async = false, int timeout=-1)
        {
            if (IsAlive)
                throw new Exception("Please stop the websocket first.");

            if(timeout<0)
                timeout = WaitTimeout;

            CreateClient();

            // excpetion will be thrown on errors.
            AsyncOperationWithTimeout(() =>
            {
                WSClient.Open();
                if (!async)
                    while (WSClient.State != WebSocket4Net.WebSocketState.Open)
                        System.Threading.Thread.Sleep(1);
            }, timeout);
        }

        #endregion

        #region event helper classes

        public class WSArgs : EventArgs
        {
            /// <summary>
            /// Construct a new websocket event.
            /// </summary>
            /// <param name="wsID">The websocket associated with event.</param>
            public WSArgs (string wsID)
            {
                WebsocketID = wsID;
            }

            /// <summary>
            /// The websocket associated with event.
            /// </summary>
            public string WebsocketID { get; set; }
        }

        public class ErrorArgs : WSArgs
        {
            public ErrorArgs(string webocketID, Exception err = null)
                : base(webocketID)
            {
                Error = err;
            }

            public Exception Error { get; set; }
        }

        public class MessageArgs : WSArgs
        {
            public MessageArgs(string wsID, string message, byte[] data)
                : base(wsID)
            {
                Message = message;
                Data = data;
            }

            public string Message { get; set; }
            public byte[] Data { get; set; }
        }

        public class PingArgs : WSArgs
        {
            public PingArgs(string wsID, byte[] data)
                : base(wsID)
            {
                Data = data;
            }

            public byte[] Data { get; set; }
        }

        #endregion

        #region events

        /// <summary>
        /// Called when a message is recived.
        /// </summary>
        public event EventHandler<MessageArgs> MessageRecived;

        /// <summary>
        /// Called when an error occurs.
        /// </summary>
        public event EventHandler<ErrorArgs> Error;

        /// <summary>
        /// Called on a ping request.
        /// </summary>
        public event EventHandler<PingArgs> ServerPing;

        /// <summary>
        /// Called when a websocket is opened.
        /// </summary>
        public event EventHandler<WSArgs> Opened;

        /// <summary>
        /// Called when a websocket is closed.
        /// </summary>
        public event EventHandler<WSArgs> Closed;

        #endregion

        #region event handling

        object m_messageRecivedProcessLock = new object();

        void OnMessageRecived(string msg, byte[] data, string id)
        {
            if (MessageRecived == null)
                return;

            try
            {
                AsyncOperationWithTimeout(() => { MessageRecived(this, new MessageArgs(id, msg, data)); });
            }
            catch (Exception ex) { ThrowOrInvokeError(id, ex); }
        }

        void OnErrorRecived(Exception ex, string id)
        {
            if (Error == null)
                return;
            Error(this, new ErrorArgs(id, ex));
        }

        void OnClose(string id)
        {
            if (Closed == null)
                return;
            try
            {
                AsyncOperationWithTimeout(() => { Closed(this, new WSArgs(id)); });
            }
            catch (Exception ex) { ThrowOrInvokeError(id, ex); }
        }

        void OnOpen(string id)
        {
            if (Opened == null)
                return;

            try
            {
                AsyncOperationWithTimeout(() => { Opened(this, new WSArgs(id)); });
            }
            catch (Exception ex) { ThrowOrInvokeError(id, ex); }
        }

        void OnServerPing(byte[] data, string id)
        {
            if (ServerPing == null)
                return;
            try
            {
                AsyncOperationWithTimeout(() => { ServerPing(this, new PingArgs(id, data)); });
            }
            catch (Exception ex) { ThrowOrInvokeError(id, ex); }
        }

        #endregion

        #region Operations

        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="id">If this websocket is listening (is a sever), you can specific a collection of client to send to.</param>
        public void Send(string msg, string id = null)
        {
            Send((object)msg, id == null ? null : new string[] { id });
        }

        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="id">If this websocket is listening (is a sever), you can specific a collection of client to send to.</param>
        public void Send(byte[] msg, string id = null)
        {
            Send((object)msg, id==null ? null :  new string[] { id });
        }

        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="ids">If this websocket is listening (is a sever), you can specific a collection of clients to send to.</param>
        public void SendMulti(string msg, string[] ids = null)
        {
            Send((object)msg, ids);
        }

        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="id">If this websocket is listening (is a sever), you can specific a collection of clients to send to.</param>
        public void SendMulti(byte[] data, string[] ids = null)
        {
            Send((object)data, ids);
        }

        private Object m_sendLock = new Object();
        private void Send(object msg, string[] ids)
        {
            if (!IsAlive)
                throw new Exception("cannot send a message since the websocket is not active. Please either Connect() or Listen().");

            lock (m_sendLock)
            {
                if (IsListening)
                {
                    ServerSend(msg, ids);
                }
                else
                {
                    ClientSend(msg);
                }
            }
        }

        private void ClientSend(object msg)
        {
            AsyncOperationWithTimeout(() =>
            {
                // Send as client.
                if (msg is string)
                    WSClient.Send((string)msg);
                else WSClient.Send((byte[])msg, 0, ((byte[])msg).Length);
            });
        }

        private void ServerSend(object msg, string[] ids)
        {
            IEnumerable<Fleck.IWebSocketConnection> cons = FindConnections(ids);

            Fleck.IWebSocketConnection lastCon = null;

            AsyncOperationWithTimeout(() =>
             {
                 // Sending the message.
                 if (msg is string)
                     foreach (var con in cons)
                     {
                         lastCon = con;
                         con.Send((string)msg);
                     }
                 else foreach (var con in cons)
                     {
                         lastCon = con;
                         con.Send((byte[])msg);
                     }
             });
        }

        /// <summary>
        /// Returns a collection of valid ids (ones which are currently connected.
        /// </summary>
        /// <param name="ids">If null returns all the curreent ids.</param>
        /// <returns></returns>
        public string[] getValidConnectionIDs(string[] ids)
        {
            if (ids == null)
                return ServerConnections.Keys.ToArray();
            return ServerConnections.Keys.Intersect(ids).ToArray();
        }

        /// <summary>
        /// Returns a collection of connections with the specified id.
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        private IEnumerable<Fleck.IWebSocketConnection> FindConnections(string[] ids)
        {
            // Send as server.
            IEnumerable<Fleck.IWebSocketConnection> cons = null;
            if (ids == null)
                cons = ServerConnections.Values;
            else
            {
                var idsHash = ids == null ? null : new HashSet<string>(ids);
                cons = ServerConnections.Where(kvp => idsHash.Contains(kvp.Key)).Select(kvp => kvp.Value).ToArray();
            }

            return cons;
        }

        /// <summary>
        /// Call to disconnect/stop listening.
        /// </summary>
        public void Stop()
        {
            if (!IsAlive)
                return;
            if (IsListening)
            {
                WSServer.ListenerSocket.Close();
                ServerConnections.Clear();
            }
            else WSClient.Close();
        }


        #endregion

        #region Helper methods

        /// <summary>
        /// Helper for async operation with timeout.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="timeout"></param>
        private void AsyncOperationWithTimeout(Action a, int timeout = -1)
        {
            if (timeout < 0)
                timeout = WaitTimeout;

            WebsocketPipeExtentions.AsyncOperationWithTimeout(a, timeout);
        }

        /// <summary>
        /// Either throws or invokes an error, according to if an error handler has been added.
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="forceThrowErrors"></param>
        private void ThrowOrInvokeError(string wsID, Exception ex, bool forceThrowErrors = false)
        {
            if (!forceThrowErrors && Error != null)
                Error(this, new ErrorArgs(wsID, ex));
            else throw ex;
        }

        #endregion

        #region IDisposeable

        public void Dispose()
        {
            Stop();
            if (WSServer != null)
                WSServer.Dispose();
            if (WSClient != null)
                WSClient.Dispose();
        }

        #endregion
    }
}
