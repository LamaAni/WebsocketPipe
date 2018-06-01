using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServerTesterConsole
{
    class Program
    {
        static WebsocketPipe.WebsocketPipe<byte[]> TestServer;
        static WebsocketPipe.WebsocketPipe<byte[]> InternalClient;

        static void Main(string[] args)
        {
            Console.WriteLine("Preparing data...");
            
            int pixelNumberOfBytes = 4;
            int imgWidth = 10;
            int numberOfSends = 100;
            int imgHeight = 1;
            bool usePipe = true;
            bool requestResponses = true;
            bool useTimeouts = false;

            var url = new Uri("ws://localhost:8000/Tester");
            WebsocketPipe.IWebsocketPipeDataSocket datasocket;
            if (usePipe)
                datasocket = new WebsocketPipe.WebsocketPipeMemoryMappedFileDataSocket();
            else datasocket = new WebsocketPipe.WebsocketPipeMSGInternalDataSocket();
            var dataToSend = new byte[imgWidth * imgHeight * pixelNumberOfBytes];

            TestServer = new WebsocketPipe.WebsocketPipe<byte[]>(url, datasocket);
            TestServer.LogMethod = (id, s) => Console.WriteLine(s);

            Console.WriteLine("Creating server..");
            TestServer.MessageRecived += TestServer_MessageRecived;
            if (!useTimeouts)
                TestServer.Timeout = -1;
            TestServer.Listen();

            if (true)
            {
                Console.WriteLine("Creating internal client and testing..");
                InternalClient = new WebsocketPipe.WebsocketPipe<byte[]>(url, datasocket);
                InternalClient.MessageRecived += InternalClient_MessageRecived;
                if (!useTimeouts)
                    InternalClient.Timeout = -1;
                InternalClient.Connect();

                // Testing send from server.
                Console.WriteLine("Testing send from server (with response).");
                TestServer.Send(dataToSend, (rsp) =>
                {
                    Console.WriteLine("Recived a response from the client with length " + (rsp == null ? 0 : rsp.Length));
                });

                Console.WriteLine("Testing send from client (with response).");
                InternalClient.Send(dataToSend, (rsp) =>
                {
                    Console.WriteLine("Recived a response from the server with length " + (rsp == null ? 0 : rsp.Length));
                });

                //Stopwatch watch = new Stopwatch();
                //System.IO.MemoryStream ms = new System.IO.MemoryStream();

                // writing first to prepare the serializer.
               
                //int numberOfSerializations = numberOfSends;

                //ms.Seek(0, System.IO.SeekOrigin.Begin);
                //InternalClient.Serializer.WriteTo(ms, dataToSend);
                //ms.Seek(0, System.IO.SeekOrigin.Begin);
                //InternalClient.Serializer.ReadFrom(ms);
                //watch.Start();
                //for (int i = 0; i < numberOfSerializations; i++)
                //{
                //    ms.Seek(0, System.IO.SeekOrigin.Begin);
                //    InternalClient.Serializer.WriteTo(ms, dataToSend);
                //    ms.Seek(0, System.IO.SeekOrigin.Begin);
                //    InternalClient.Serializer.ReadFrom(ms);
                //}

                //watch.Stop();
                //double serTime = watch.Elapsed.TotalMilliseconds / numberOfSerializations;
                //Console.WriteLine("Serialization time: " + serTime);

                //Action<byte[]> response = null;
                //int numberOfValidResponses = 0;
                //if (requestResponses)
                //    response = (rsp) =>
                //    {
                //        if (rsp != null) numberOfValidResponses++;
                //    };
                //watch.Reset();
                //watch.Start();
                
                //for (int i = 0; i < numberOfSends; i++)
                //    InternalClient.Send(dataToSend, response);

                //int numberOfSleeps = 0;
                //while (totalRecivedCount < numberOfSends)
                //{
                //    numberOfSleeps++;
                //    System.Threading.Thread.Sleep(1);
                //}

                //watch.Stop();

                //Console.WriteLine("Has slept " + numberOfSleeps + " times.");
                //Console.WriteLine("Valid resp object count: " + numberOfValidResponses);

                //Console.WriteLine("Total transaction:" + watch.Elapsed.TotalMilliseconds);
                //Console.WriteLine("Total transaction per call:" + watch.Elapsed.TotalMilliseconds / numberOfSends);
                //Console.WriteLine("Total transaction pipe overhead:" + (watch.Elapsed.TotalMilliseconds / numberOfSends - serTime));

                if(usePipe)
                {
                    var mmfds=datasocket as WebsocketPipe.WebsocketPipeMemoryMappedFileDataSocket;
                    Console.WriteLine("Total memory mapped file creations: " + mmfds.TotalNumberOfMemoryMappedFilesCreated);
                    Console.WriteLine("Total active memory mapped file: " + mmfds.TotalActiveMemoryMappedFiles);
                }

                InternalClient.Stop();
            }            

            Console.WriteLine("Listening to service at :" + TestServer.Address.ToString());
            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();

            Console.Write("Stopping the server ... ");
            TestServer.Stop();
            TestServer.Dispose();
            Console.WriteLine("OK.");
        }

        private static void InternalClient_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<byte[]>.MessageEventArgs e)
        {
            Console.WriteLine("Recived at client " + e.Message.Length + " bytes");
            if (e.RequiresResponse)
                e.Response = e.Message;
        }

        private static void TestServer_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<byte[]>.MessageEventArgs e)
        {
            Console.WriteLine("Recived at server " + e.Message.Length + " bytes");
            if (e.RequiresResponse)
                e.Response = e.Message;
        }
    }
}
