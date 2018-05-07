using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClientTesterConsole
{
    class Program
    {
        static WebsocketPipe.WebsocketPipe<string> ClientTester;

        static void Main(string[] args)
        {
            ClientTester = new WebsocketPipe.WebsocketPipe<string>(
                new Uri("ws://localhost:8000/Tester"));
            ClientTester.Connect();
            ClientTester.Send("This is a message that was sent throught a piped channel.");

            ClientTester.Disconnect();
            ClientTester.Dispose();

            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();
        }
    }
}
