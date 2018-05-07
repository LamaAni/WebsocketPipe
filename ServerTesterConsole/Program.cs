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
        static WebsocketPipe.WebsocketPipe<string> TestServer;
        static WebsocketPipe.WebsocketPipe<string> InternalClient;

        static void Main(string[] args)
        {
            Console.WriteLine("Preparing data...");
            var url = new Uri("ws://localhost:8000/Tester");
            int pixelNumberOfBytes = 8;
            int imgWidth = 3000;
            int imgHeight = imgWidth;
            bool usePipe = true;
            
            var dataToSend = new string('0', imgWidth * imgHeight * pixelNumberOfBytes);

            WebsocketPipe.IWebsocketPipeDataSocket<string> datasocket;
            if (usePipe)
                datasocket = new WebsocketPipe.WebsocketPipeMemoryMappedFileDataSocket<string>();
            else datasocket = new WebsocketPipe.WebsocketPipeMSGInternalDataSocket<string>();

            TestServer = new WebsocketPipe.WebsocketPipe<string>(url, datasocket);

            Console.WriteLine("Creating server..");
            TestServer.MessageRecived += TestServer_MessageRecived;
            TestServer.Listen();
            
            if (true)
            {
                Console.WriteLine("Creating internal client and testing..");
                InternalClient = new WebsocketPipe.WebsocketPipe<string>(url, datasocket);
                InternalClient.Connect();

                Stopwatch watch = new Stopwatch();
                System.IO.MemoryStream ms = new System.IO.MemoryStream();

                // writing first to prepare the serializer.
                int numberOfSends = 100;
                int numberOfSerializations = numberOfSends;

                ms.Seek(0, System.IO.SeekOrigin.Begin);
                InternalClient.Serializer.WriteMessage(ms, dataToSend);
                ms.Seek(0, System.IO.SeekOrigin.Begin);
                InternalClient.Serializer.ReadMessage(ms);
                watch.Start();
                for (int i = 0; i < numberOfSerializations; i++)
                {
                    ms.Seek(0, System.IO.SeekOrigin.Begin);
                    InternalClient.Serializer.WriteMessage(ms, dataToSend);
                    ms.Seek(0, System.IO.SeekOrigin.Begin);
                    InternalClient.Serializer.ReadMessage(ms);
                }
                watch.Stop();
                double serTime = watch.Elapsed.TotalMilliseconds / numberOfSerializations;
                Console.WriteLine("Serialization time: " + serTime);
                watch.Reset();
                watch.Start();
                

                for (int i = 0; i < numberOfSends; i++)
                    InternalClient.Send(dataToSend);

                while (totalRecivedCount < numberOfSends)
                    System.Threading.Thread.Sleep(1);
                watch.Stop();
                Console.WriteLine("Total transaction:" + watch.Elapsed.TotalMilliseconds);
                Console.WriteLine("Total transaction per call:" + watch.Elapsed.TotalMilliseconds / numberOfSends);
                Console.WriteLine("Total transaction pipe overhead:" + (watch.Elapsed.TotalMilliseconds / numberOfSends - serTime));
            }

            Console.WriteLine("Listening to service at :" + TestServer.Address.ToString());
            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();
        }


        static int totalRecivedCount=0;
        private static void TestServer_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<string>.MessageEventArgs e)
        {
            //Console.WriteLine("Message recived, here it is: ");
            totalRecivedCount++;
            //Console.WriteLine(e.Message);
        }
    }
}
