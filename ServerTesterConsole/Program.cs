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
            int imgHeight = imgWidth;
            bool usePipe = true;
            bool requestResponses = true;

            var url = new Uri("ws://localhost:8000/Tester");
            WebsocketPipe.IWebsocketPipeDataSocket datasocket;
            if (usePipe)
                datasocket = new WebsocketPipe.WebsocketPipeMemoryMappedFileDataSocket();
            else datasocket = new WebsocketPipe.WebsocketPipeMSGInternalDataSocket();
            var dataToSend = new byte[imgWidth * imgHeight * pixelNumberOfBytes];

            TestServer = new WebsocketPipe.WebsocketPipe<byte[]>(url, datasocket);
            TestServer.LogMethod = (d, s) => Console.WriteLine(d.ToString());

            Console.WriteLine("Creating server..");
            TestServer.MessageRecived += TestServer_MessageRecived;
            TestServer.Listen();
            
            if (true)
            {
                Console.WriteLine("Creating internal client and testing..");
                InternalClient = new WebsocketPipe.WebsocketPipe<byte[]>(url, datasocket);
                InternalClient.MessageRecived += InternalClient_MessageRecived;
                InternalClient.Connect();

                Console.WriteLine("Sending single message with response. (Will wait for it...)");
                InternalClient.Send(dataToSend, (rsp) => {
                    Console.WriteLine("Recived a response from the server with length " + rsp.Length);
                });

                Stopwatch watch = new Stopwatch();
                System.IO.MemoryStream ms = new System.IO.MemoryStream();

                // writing first to prepare the serializer.
                int numberOfSends = 1000;
                int numberOfSerializations = numberOfSends;

                ms.Seek(0, System.IO.SeekOrigin.Begin);
                InternalClient.Serializer.WriteTo(ms, dataToSend);
                ms.Seek(0, System.IO.SeekOrigin.Begin);
                InternalClient.Serializer.ReadFrom(ms);
                watch.Start();
                for (int i = 0; i < numberOfSerializations; i++)
                {
                    ms.Seek(0, System.IO.SeekOrigin.Begin);
                    InternalClient.Serializer.WriteTo(ms, dataToSend);
                    ms.Seek(0, System.IO.SeekOrigin.Begin);
                    InternalClient.Serializer.ReadFrom(ms);
                }

                watch.Stop();
                double serTime = watch.Elapsed.TotalMilliseconds / numberOfSerializations;
                Console.WriteLine("Serialization time: " + serTime);

                Action<byte[]> response = null;
                if (requestResponses)
                    response = (rsp) => { };
                watch.Reset();
                watch.Start();
                
                for (int i = 0; i < numberOfSends; i++)
                    InternalClient.Send(dataToSend, response);

                while (totalRecivedCount < numberOfSends)
                    System.Threading.Thread.Sleep(1);

                watch.Stop();

                Console.WriteLine("Total transaction:" + watch.Elapsed.TotalMilliseconds);
                Console.WriteLine("Total transaction per call:" + watch.Elapsed.TotalMilliseconds / numberOfSends);
                Console.WriteLine("Total transaction pipe overhead:" + (watch.Elapsed.TotalMilliseconds / numberOfSends - serTime));

                if(usePipe)
                {
                    var mmfds=datasocket as WebsocketPipe.WebsocketPipeMemoryMappedFileDataSocket;
                    Console.WriteLine("Total memory mapped file creations: " + mmfds.TotalNumberOfMemoryMappedFilesCreated);
                    Console.WriteLine("Total active memory mapped file: " + mmfds.TotalActiveMemoryMappedFiles);
                }

                InternalClient.Disconnect();
            }

            pingpong = true;
            Console.WriteLine("Listening to service at :" + TestServer.Address.ToString());
            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();

            Console.Write("Stopping the server ... ");
            TestServer.StopListening();
            TestServer.Dispose();
            Console.WriteLine("OK.");
        }

        private static void InternalClient_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<byte[]>.MessageEventArgs e)
        {
            Console.WriteLine("Recived from server " + e.Message.Length + " bytes");
        }

        static bool pingpong = false;
        static int totalRecivedCount=0;
        private static void TestServer_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<byte[]>.MessageEventArgs e)
        {
            if (e.RequiresResponse)
                e.Response = new byte[2];

            if(pingpong)
            {
                Console.WriteLine("Ping at server with " + e.Message.Length + " bytes");
                TestServer.Send(e.Message);
                return;
            }
            totalRecivedCount++;
        }
    }
}
