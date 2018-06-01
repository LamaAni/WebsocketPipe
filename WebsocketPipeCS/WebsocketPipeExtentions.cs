using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebsocketPipe.Extentions
{
    /// <summary>
    /// Adds task extentions
    /// </summary>
    public static class WebsocketPipeExtentions
    {
        public static void AsyncOperationWithTimeout(this Action a, int timeout)
        {
            if (timeout <= 0)
                throw new Exception("Timeout must at lease be 1 ms.");

            Task t = new Task(a);
            t.Start();
            bool rt = t.Wait(timeout);
            if (t.IsFaulted)
                throw t.Exception;
        }
    }
}
