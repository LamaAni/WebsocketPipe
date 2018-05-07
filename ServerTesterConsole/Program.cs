using System;
using System.Collections.Generic;
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
            var url = new Uri("ws://localhost:8000/Tester");
            TestServer = new WebsocketPipe.WebsocketPipe<string>(url);

            TestServer.MessageRecived += TestServer_MessageRecived;
            TestServer.Listen();

            if(true)
            {
                InternalClient = new WebsocketPipe.WebsocketPipe<string>(url);
                InternalClient.Connect();
                InternalClient.Send("This is a test message... wowho.");
            }

            Console.WriteLine("Listening to service at :" + TestServer.Address.ToString());
            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();
        }

        private static void TestServer_MessageRecived(object sender, WebsocketPipe.WebsocketPipe<string>.MessageEventArgs e)
        {
            Console.WriteLine("Message recived, here it is: ");
            Console.WriteLine(e.Message);
        }
    }
}
