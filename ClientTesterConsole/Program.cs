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
            var dataToSend = new byte[1000];
            ClientTester = new WebsocketPipe.WebsocketPipe<byte[]>(
                new Uri("ws://localhost:8000/Tester"),
                new WebsocketPipe.WebsocketPipeBinaryFormatingDataSerializer<byte[]>());

            ClientTester.Connect();
            ClientTester.MessageRecived += ClientTester_MessageRecived;
            ClientTester.Send(dataToSend, (a, b) => { });
            
            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();

            ClientTester.Disconnect();
            ClientTester.Dispose();
        }

        private static void ClientTester_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<byte[]>.MessageEventArgs e)
        {
            // pingpong.
            Console.WriteLine("Recived back " + e.Message.Length + " bytes.");
        }
    }
}
