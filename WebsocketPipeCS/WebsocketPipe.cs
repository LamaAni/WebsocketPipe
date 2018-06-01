using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WebsocketPipe.Extentions;

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


            Websocket = new WebsocketPipeWS(address);
            Serializer = serializer;
            DataSocket = dataSocket;

            // binding websocket methods.
            LogMethod = (id, s) => { };

            // bind events.
            BindWebsocketEvents();
        }

        #endregion

        #region General Properties

        /// <summary>
        /// Random generated id to allow for multiple mmf on the same connection name.
        /// </summary>
        public string PipeID { get { return Websocket.ID; } }

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

        #endregion

        #region Connection properties

        /// <summary>
        /// The address to send to.
        /// </summary>
        public Uri Address { get { return Websocket.Address; } }

        /// <summary>
        /// The websocket used for communication.
        /// </summary>
        public WebsocketPipe.WebsocketPipeWS Websocket { get; private set; }

        /// <summary>
        /// The timespan to wait before throwing timeout.
        /// </summary>
        public TimeSpan WaitTimeout
        {
            get
            {
                return TimeSpan.FromMilliseconds(Timeout);
            }
            set
            {
                Timeout = Convert.ToInt32(value.TotalMilliseconds);
            }
        }

        /// <summary>
        /// The time to wait before timeout, in ms.
        /// </summary>
        public int Timeout
        {
            get
            {
                return Websocket.WaitTimeout;
            }
            set
            {
                Websocket.WaitTimeout = value;
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

        #region logging

        /// <summary>
        /// The miminmal delay time for which timed messages will be logged (for example serialization)
        /// time which is longer then x.
        /// </summary>
        public int MinTimeForLogTimedMessages { get; set; } = 20;

        /// <summary>
        /// If true then log websocket messages.
        /// </summary>
        public bool LogWebsocketMessages { get; set; }


        public delegate void LogMethodDelegate(string websocketID, string message);
        /// <summary>
        /// Method to be called on log.
        /// </summary>
        public LogMethodDelegate LogMethod { get; set; }

        /// <summary>
        /// Call to log a message. 
        /// </summary>
        /// <param name="websocketID"></param>
        /// <param name="msg"></param>
        protected void WriteLogMessage(string websocketID, string msg)
        {
            CallLogMethod(websocketID, DateTime.Now.ToString() + " WSP\t| " + msg);
        }

        /// <summary>
        /// Write log for timed messages.
        /// </summary>
        /// <param name="websocketID"></param>
        /// <param name="msg"></param>
        /// <param name="ms"></param>
        /// <param name="ifOnlyAboveMinTime"></param>
        protected void WriteTimedLogMessage(string websocketID, string msg, double ms, bool ifOnlyAboveMinTime=true)
        {
            if (!ifOnlyAboveMinTime || ms >= MinTimeForLogTimedMessages)
                WriteLogMessage(websocketID, msg + ", dt [ms]: " + ms);
        }

        private void WebsocketLogMessage(string websocketID, string msg)
        {
            if(LogWebsocketMessages)
            {
                CallLogMethod(websocketID, msg);
            }
        }

        private void CallLogMethod(string websocketID, string msg)
        {
            if (LogMethod != null)
            {
                LogMethod(websocketID, msg);
            }
        }

        #endregion

        #region connection methods

        /// <summary>
        /// Listens (and creates a server if needed) to remote connections.
        /// </summary>
        public void Listen()
        {
            Websocket.Listen();
            WriteLogMessage(PipeID, "Websocket pipe " + PipeID + " is listening for conenctions.");
        }

        /// <summary>
        /// Either stop listenting or disconnect from the server depending if this is a client or a server.
        /// </summary>
        public void Stop()
        {
            Websocket.Stop();
            WriteLogMessage(PipeID, "Stopping websocket pipe " + PipeID + " ");
        }

        /// <summary>
        /// Connects to the remote server as a client.
        /// </summary>
        /// <param name="address"></param>
        public void Connect(bool async = false)
        {
            Websocket.Connect();
            WriteLogMessage(PipeID, "Websocket pipe " + PipeID + " is connected to remote.");
        }

        public bool IsAlive { get { return Websocket.IsAlive; } }
        public bool IsListening { get { return Websocket.IsListening; } }
        public bool IsConnected { get { return Websocket.IsConnected; } }

        #endregion

        #region message processing (WebsocketSharp)

        private void BindWebsocketEvents()
        {
            Websocket.Closed += Websocket_Closed;
            Websocket.Error += Websocket_Error;
            Websocket.Opened += Websocket_Opened;
            Websocket.ServerPing += Websocket_ServerPing;
            Websocket.MessageRecived += Websocket_MessageRecived;
        }

        private void Websocket_MessageRecived(object sender, WebsocketPipeWS.MessageArgs e)
        {
            if (e.Data != null)
                OnDataRecived(e.Data, e.WebsocketID);

            if (e.Message != null)
                WriteLogMessage(e.WebsocketID, e.Message);
        }

        private void Websocket_Opened(object sender, WebsocketPipeWS.WSArgs e)
        {
            if (Open != null)
                Open(this, new MessageEventArgs(null, false, e.WebsocketID));

            WriteLogMessage(e.WebsocketID, "WS " + e.WebsocketID + ": Connection opend.");
        }

        private void Websocket_ServerPing(object sender, WebsocketPipeWS.PingArgs e)
        {
            if (Ping != null)
            {
                Ping(this, new MessageEventArgs(null, false, e.WebsocketID));
            }
            WriteLogMessage(e.WebsocketID, "WS " + e.WebsocketID + ": ping (" + e.Data.Length + ")");
        }

        private void Websocket_Error(object sender, WebsocketPipeWS.ErrorArgs e)
        {
            // just throw the error.
            WriteLogMessage(e.WebsocketID, e.Error.ToString());
            WriteLogMessage(e.WebsocketID, "WS " + e.WebsocketID + ": Error, " + e.Error.ToString());
            if (Error == null)
                throw e.Error;
            else Error(this, e);
        }

        private void Websocket_Closed(object sender, WebsocketPipeWS.WSArgs e)
        {
            var id = e.WebsocketID;
            TriggerWaitHandle(id, null);

            OnClose(new MessageEventArgs(null, false, id));
            WriteLogMessage(id, "WS " + id + ": connection closed.");
        }

        private bool TriggerWaitHandle(string datasocketID, TMessage msg)
        {
            if (PendingResponseWaitHandles.ContainsKey(datasocketID))
            {
                ResponseWaitHandle hndl = PendingResponseWaitHandles[datasocketID];
                PendingResponseWaitHandles.Remove(datasocketID);
                hndl.Response = msg;
                hndl.Set();
                return true;
            }
            return false;
        }

        protected void OnDataRecived(byte[] data, string id)
        {
            string datasocketId = ToDataSocketID(id);

            TotalMessageRecivedEvents++;
            MemoryStream ms = new MemoryStream(data);
            WebsocketPipeMessageInfo[] msgs;

            System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            try
            {
                msgs = DataSocket.ReadMessages(ms).ToArray();
            }
            catch(Exception ex)
            {
                string msg = "Error while reading messages from data socket: " + ex.Message;
                WriteLogMessage(id, msg);
                throw new Exception(msg, ex);
            }

            watch.Stop();
            WriteTimedLogMessage(id, "Read from datasocket",watch.Elapsed.TotalMilliseconds);

            ms.Close();
            ms.Dispose();
            ms = null;

            watch.Reset();
            watch.Start();
            int bytecount = 0;

            MessageEventArgs[] mes = msgs.Select(msg =>
            {
                bytecount = bytecount + msg.Data.Length;
                TMessage o;
                try
                {
                    o = Serializer.FromBytes(msg.Data); ;
                }
                catch (Exception ex)
                {
                    var str = "Error desrializing message. " + ex.Message;
                    WriteLogMessage(id,str);
                    throw new Exception(str, ex);
                }

                return new MessageEventArgs(o, msg.RequiresResponse, id);
            }).ToArray();

            WriteTimedLogMessage(id, "Deserialzed " + mes.Length + " messages with " + bytecount + " [bytes] ", watch.Elapsed.TotalMilliseconds);

            watch.Stop();
            watch.Reset();
            watch.Start();
            foreach (var me in mes)
            {
                if (TriggerWaitHandle(datasocketId, me.Message)) // check if the message was a response.
                    continue;

                // call the message handler.
                me.NumberOfMessagesInBlock = mes.Length;
                OnMessage(me);
            }
            watch.Stop();
            WriteTimedLogMessage(id, "Handled evnets for " + mes.Length + " messages", 1000);
        }

        #endregion

        #region Async Stack

        /// <summary>
        /// The maximal number of consecutive thread operations for a websocket id. For a listening socket multiple id agents are available.
        /// </summary>
        public int MaxStackSize { get; private set; } = 100;

        // Process a stacked opeation.
        Dictionary<string, HashSet<Task>> m_threadStack = new Dictionary<string, HashSet<Task>>();

        /// <summary>
        /// Make an asynked stacked operation that allows multithread operation to execute without blocking.
        /// Maximal stack depath is determined by the MaxStackSize. 
        /// </summary>
        /// <param name="id">The websocket id to associate the operation with</param>
        /// <param name="a">The action to take</param>
        /// <param name="timeout">Timeout for the operation</param>
        /// <returns></returns>
        private void StackedThreadOperation(string id, Action a, int timeout = -1)
        {
            if (timeout < 0)
                timeout = Timeout;

            if (!m_threadStack.ContainsKey(id))
                m_threadStack[id] = new HashSet<Task>();

            if (m_threadStack[id].Count > MaxStackSize)
                ThrowOrInvokeError(id, new StackOverflowException(
                    "The maximal number of concurrent thread for websocket " + id + ", " + MaxStackSize + ", has been reached."));

            Task t = null;
            t = new Task(() =>
            {
                // call the async operation with a timeout.
                try { AsyncOperationWithTimeout(a, timeout); }
                // catch timout or other errors.
                catch (Exception ex) { ThrowOrInvokeError(id, ex); }
                // remove the thread from the collection.
                finally
                {
                    lock (m_threadStack)
                    {
                        if (m_threadStack.ContainsKey(id))
                        {
                            if (m_threadStack[id].Contains(t))
                                m_threadStack[id].Remove(t);
                            if (m_threadStack.Count == 0)
                            {
                                m_threadStack.Remove(id);
                            }
                        }
                    }
                }
            });

            m_threadStack[id].Add(t);
            t.Start();
        }

        #endregion

        #region Threading Helper methods

        /// <summary>
        /// Helper for async operation with timeout.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="timeout"></param>
        private void AsyncOperationWithTimeout(Action a, int timeout = -1)
        {
            if (timeout < 0)
                timeout = Timeout;

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
                Error(this, new WebsocketPipeWS.ErrorArgs(wsID, ex));
            else throw ex;
        }

        #endregion

        #region Message processing

        protected virtual void OnMessage(MessageEventArgs e)
        {
            if (MessageRecived == null)
                return;

            StackedThreadOperation(e.WebsocketID, () =>
            {
                MessageRecived(this, e);

                if (e.RequiresResponse)
                {
                    Send(e.Response, e.WebsocketID);
                }
            });
        }

        protected virtual void OnClose(MessageEventArgs e)
        {
            if (Close != null)
                Close(this, e);
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
            if (clientId != null)
                clientId = clientId.Length == 0 ? null : clientId;

            Send(msg, clientId == null ? null : new string[] { clientId }, response);
        }

        private Object m_sendLock = new Object();
        /// <summary>
        /// Sends a message to sepcific clients by the client id. Method should be used for servers.
        /// </summary>
        /// <param name="msg">The message to send</param>
        /// <param name="clientIds">The client ids to send the msg to, if a server.</param>
        /// /// <param name="rsp">If not null, the thread will wait for response.</param>
        public void Send(TMessage msg, string[] clientIds, Action<TMessage> rsp = null)
        {
            //if (clientIds != null && WS != null)
            //    throw new Exception("You are trying to send a message to specific clients from a client WebsocketPipe. This is not A server.");
            if(!IsAlive)
                throw new Exception("Not connected to any server or listening for connections. Please invoke either Connect(), or Listen().");

            Stopwatch watch = new Stopwatch();
            WebsocketPipeMessageInfo minfo;
            watch.Start();
            try
            {
                minfo = new WebsocketPipeMessageInfo(Serializer.ToBytes<TMessage>(msg), null, rsp != null);
            }
            catch (Exception ex)
            {
                var str = "Error serializing message. " + ex.Message;
                WriteLogMessage(null,str);
                throw new Exception(str, ex);
            }

            watch.Stop();

            WriteTimedLogMessage(null, "Serialized msg with " + minfo.Data.Length + " [bytes]", watch.Elapsed.TotalMilliseconds);

            var senders = new[] {new
            {
                dataSocketID = "",
                websocketID="",
                hndl = (ResponseWaitHandle)null,
            }}.ToList(); senders.Clear();


            if (IsConnected)
            {
                senders.Add(new
                {
                    dataSocketID = ToDataSocketID(Websocket.ID),
                    websocketID = Websocket.ID,
                    hndl = new ResponseWaitHandle(),
                });
            }
            else
            {
                foreach (string id in Websocket.getValidConnectionIDs(clientIds))
                {
                    senders.Add(new
                    {
                        dataSocketID = ToDataSocketID(id),
                        websocketID = id,
                        hndl = new ResponseWaitHandle(),
                    });
                }
            }

            if (senders.Count == 0)
            {
                if (rsp != null)
                    rsp(null);
                return; // nothing to do.
            }

            lock (m_sendLock)
            {
                if (rsp == null)
                {
                    foreach (var sender in senders)
                    {
                        minfo.DataSocketId = sender.dataSocketID;
                        Websocket.Send(GetWebsocketMessageData(minfo), sender.websocketID);
                    }
                }
                else
                {
                    foreach (var sender in senders)
                    {
                        PendingResponseWaitHandles[sender.dataSocketID] = sender.hndl;
                    }

                    foreach (var sender in senders)
                    {
                        minfo.DataSocketId = sender.dataSocketID;
                        Websocket.Send(GetWebsocketMessageData(minfo), sender.websocketID);
                    }

                    bool timedout = false;
                    foreach (var sender in senders)
                        if (!sender.hndl.WaitOne(WaitTimeout))
                            timedout = true;

                    if (timedout)
                        throw new Exception("Timedout waiting for response. Waited [ms] " + WaitTimeout.TotalMilliseconds);

                    foreach (var sender in senders)
                        rsp(sender.hndl.Response);
                }
            }
        }

        private string ToDataSocketID(string id)
        {
            return PipeID + "_" + id;
        }

        private void doSend(TMessage msg, Action<bool, string> asyncOnComplete, string id)
        {
        }

        private byte[] GetWebsocketMessageData(WebsocketPipeMessageInfo msg)
        {
            MemoryStream strm = new MemoryStream();
            System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            try
            {
                DataSocket.WriteMessage(msg, strm);
            }
            catch (Exception ex)
            {
                var str = "Error while writing to data socket: " + ex.Message;
                WriteLogMessage(null, str);
                throw new Exception(str, ex);
            }

            watch.Stop();
            WriteTimedLogMessage(null, "Write to datasocket, ", watch.Elapsed.TotalMilliseconds);

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
            public MessageEventArgs(TMessage msg, bool needsResponse, string websocketID)
            {
                Message = msg;
                RequiresResponse = needsResponse;
                WebsocketID = websocketID;
            }

            /// <summary>
            /// If true this message requires a response of type TMessage. Can be null.
            /// </summary>
            public bool RequiresResponse { get; private set; }

            /// <summary>
            /// The message.
            /// </summary>
            public TMessage Message { get; set; }

            /// <summary>
            /// The response to send back if one is needed.
            /// </summary>
            public TMessage Response { get; set; }

            /// <summary>
            /// The total number of messages that were sent in the current message block.
            /// </summary>
            public int NumberOfMessagesInBlock { get; internal set; } = 1;

            /// <summary>
            /// The id of the wensocket.
            /// </summary>
            public string WebsocketID { get; private set; }


            EventWaitHandle m_WaitHandle = null;

            /// <summary>
            /// Uses a wait handle to use in the case where the executing framework needs asynchronius events.
            /// (For example if integrated in matlab). MAKE SURE TO MARK the wait handle as complete. (set)
            /// </summary>
            public EventWaitHandle WaitHandle {
                get
                {
                    if (m_WaitHandle == null) m_WaitHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
                    return m_WaitHandle;
                }
            }

            /// <summary>
            /// Waits for asynchronius event execution
            /// </summary>
            /// <param name="doWait">if null, true in the case where a response is required.</param>
            /// <param name="timeout">If less then 1, uses the default wait timeout 10000[ms]</param>
            public void WaitForAsynchroniusEvent(bool? doWait = null, int timeout  = -1)
            {
                if (timeout < 1)
                    timeout = 1000;

                if (doWait == null)
                    doWait = RequiresResponse;

                if(doWait==true)
                {
                    if (!WaitHandle.WaitOne(timeout, false))
                        throw new Exception("Wait for messaging event (Async Events) to complete, timedout. Error.");
                }
            }

            /// <summary>
            /// Call to release the wait
            /// </summary>
            public void ReleaseAsynchroniusEvent()
            {
                if (m_WaitHandle == null)
                    return;

                m_WaitHandle.Set();
            }
        }

        /// <summary>
        /// Called when a message is recived.
        /// </summary>
        public event EventHandler<MessageEventArgs> MessageRecived;

        /// <summary>
        /// When a socket is closed.
        /// </summary>
        public event EventHandler<MessageEventArgs> Close;

        /// <summary>
        /// When a socket is closed.
        /// </summary>
        public event EventHandler<MessageEventArgs> Open;

        /// <summary>
        /// When a socket is closed.
        /// </summary>
        public event EventHandler<MessageEventArgs> Ping;

        /// <summary>
        /// When a socket is closed.
        /// </summary>
        public event EventHandler<WebsocketPipeWS.ErrorArgs> Error;

        #endregion

        #region Dispose


        public void Dispose()
        {
            Websocket.Dispose();
            DataSocket.Close();
            Websocket = null;
            DataSocket = null;
            Serializer = null;
        }

        #endregion

    }

}