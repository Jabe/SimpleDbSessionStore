using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SimpleDbSessionStore
{
    public class Global : HttpApplication
    {
        protected void Application_Start(object sender, EventArgs e)
        {
            // simple static extension point for IoC containers.
            // note that you have to dispose of injected types on your own.
            Store.DependencyResolver = type => null;
        }
    }
}