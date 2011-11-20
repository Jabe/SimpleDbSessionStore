using System;
using System.Web.UI;

namespace SimpleDbSessionStore
{
    public partial class Default : Page
    {
        protected override void OnLoad(EventArgs e)
        {
            if (Session["counter"] == null)
                Session["counter"] = 0;

            Session["counter"] = (int) Session["counter"] + 1;

            base.OnLoad(e);
        }
    }
}