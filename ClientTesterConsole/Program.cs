using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClientTesterConsole
{
    class Program
    {
        static WebsocketPipe.WebsocketPipe<byte[]> ClientTester;

        static void Main(string[] args)
        {
            var dataToSend = new byte[3000*3000];
            bool usePipe = true;
            var url = new Uri("ws://localhost:8000/Tester");
            WebsocketPipe.IWebsocketPipeDataSocket datasocket;
            if (usePipe)
                datasocket = new WebsocketPipe.WebsocketPipeMemoryMappedFileDataSocket();
            else datasocket = new WebsocketPipe.WebsocketPipeMSGInternalDataSocket();

            ClientTester = new WebsocketPipe.WebsocketPipe<byte[]>(url,datasocket);
            ClientTester.LogMethod = (id,s) =>
            {
                Console.WriteLine(s);
            };
            ClientTester.MessageRecived += ClientTester_MessageRecived;
            ClientTester.Connect();
            ClientTester.Send(dataToSend, (esp) => { });
            
            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();

            ClientTester.Stop();
            ClientTester.Dispose();
        }

        private static void ClientTester_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<byte[]>.MessageEventArgs e)
        {
            // pingpong.
            Console.WriteLine("Recived from server " + e.Message.Length + " bytes.");
        }
    }
}
