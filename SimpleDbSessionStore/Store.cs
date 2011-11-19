using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Configuration;
using System.Web.Hosting;
using System.Web.SessionState;
using Amazon;
using Amazon.SimpleDB;
using Amazon.SimpleDB.Model;
using Attribute = Amazon.SimpleDB.Model.Attribute;

namespace SimpleDbSessionStore
{
    // http://msdn.microsoft.com/en-us/library/ms178589.aspx
    public class Store : SessionStateStoreProviderBase
    {
        private static readonly CultureInfo Ic = CultureInfo.InvariantCulture;

        private AmazonSimpleDB _client;
        private string _domain;
        private string _key;
        private SessionStateSection _pConfig;
        private string _prefix;
        private string _secret;

        public override void Initialize(string name, NameValueCollection config)
        {
            if (config == null) throw new ArgumentNullException("config");

            if (String.IsNullOrEmpty(name))
            {
                name = "SdbSessionStateStore";
            }

            if (String.IsNullOrEmpty(config["description"]))
            {
                config["description"] = "SimpleDB Session Provider";
            }

            base.Initialize(name, config);

            Configuration cfg = WebConfigurationManager.OpenWebConfiguration(HostingEnvironment.ApplicationVirtualPath);
            _pConfig = (SessionStateSection) cfg.GetSection("system.web/sessionState");

            _key = config["key"];
            _secret = config["secret"];
            _domain = config["domain"];
            _prefix = config["prefix"];

            if (String.IsNullOrEmpty(_key)) throw new ArgumentException("key");
            if (String.IsNullOrEmpty(_secret)) throw new ArgumentException("secret");
            if (String.IsNullOrEmpty(_domain)) throw new ArgumentException("domain");
            if (String.IsNullOrEmpty(_prefix)) throw new ArgumentException("prefix");

            // TODO: config
            var sdbc = new AmazonSimpleDBConfig {ServiceURL = "https://sdb.eu-west-1.amazonaws.com"};

            _client = AWSClientFactory.CreateAmazonSimpleDBClient(_key, _secret, sdbc);
        }

        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(),
                                             SessionStateUtility.GetSessionStaticObjects(context), timeout);
        }

        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            var request = new PutAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                              };

            DateTimeOffset now = DateTimeOffset.UtcNow;

            Attr(request, "Created", now);
            Attr(request, "Expires", now.AddMinutes(timeout));
            Attr(request, "LockDate", now);
            Attr(request, "LockId", 0);
            Attr(request, "Timeout", timeout);
            Attr(request, "Locked", false);
            Attr(request, "SessionItems", "");
            Attr(request, "Flags", 1);

            _client.PutAttributes(request);
        }

        private static void Attr(PutAttributesRequest request, string name, object value, bool replace = true)
        {
            string str;

            if (value is DateTimeOffset)
            {
                str = ((DateTimeOffset) value).ToString("o");
            }
            else
            {
                str = Convert.ToString(value, Ic);
            }

            request.Attribute.Add(new ReplaceableAttribute {Name = name, Replace = replace, Value = str});
        }

        public override void Dispose()
        {
//CREATE TABLE Sessions
//(
//  SessionId       Text(80)  NOT NULL,
//  ApplicationName Text(255) NOT NULL,
//  Created         DateTime  NOT NULL,
//  Expires         DateTime  NOT NULL,
//  LockDate        DateTime  NOT NULL,
//  LockId          Integer   NOT NULL,
//  Timeout         Integer   NOT NULL,
//  Locked          YesNo     NOT NULL,
//  SessionItems    Memo,
//  Flags           Integer   NOT NULL,
//    CONSTRAINT PKSessions PRIMARY KEY (SessionId, ApplicationName)
//)
        }

        public override void EndRequest(HttpContext context)
        {
        }

        public override SessionStateStoreData GetItem(
            HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId,
            out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(false, context, id, out locked, out lockAge, out lockId, out actionFlags);
        }

        public override SessionStateStoreData GetItemExclusive(
            HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId,
            out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(true, context, id, out locked, out lockAge, out lockId, out actionFlags);
        }


        //
        // GetSessionStoreItem is called by both the GetItem and 
        // GetItemExclusive methods. GetSessionStoreItem retrieves the 
        // session data from the data source. If the lockRecord parameter
        // is true (in the case of GetItemExclusive), then GetSessionStoreItem
        // locks the record and sets a new LockId and LockDate.
        //

        //
        // GetSessionStoreItem is called by both the GetItem and 
        // GetItemExclusive methods. GetSessionStoreItem retrieves the 
        // session data from the data source. If the lockRecord parameter
        // is true (in the case of GetItemExclusive), then GetSessionStoreItem
        // locks the record and sets a new LockId and LockDate.
        //

        private SessionStateStoreData GetSessionStoreItem(bool lockRecord,
                                                          HttpContext context,
                                                          string id,
                                                          out bool locked,
                                                          out TimeSpan lockAge,
                                                          out object lockId,
                                                          out SessionStateActions actionFlags)
        {
            // Initial values for return value and out parameters.
            SessionStateStoreData item = null;
            lockAge = TimeSpan.Zero;
            lockId = null;
            locked = false;
            actionFlags = 0;

            // DateTime to check if current session item is expired.
            DateTimeOffset expires;
            // String to hold serialized SessionStateItemCollection.
            string serializedItems = "";
            // True if a record is found in the database.
            bool foundRecord = false;
            // True if the returned session item is expired and needs to be deleted.
            bool deleteData = false;
            // Timeout value from the data store.
            int timeout = 0;

            // lockRecord is true when called from GetItemExclusive and
            // false when called from GetItem.
            // Obtain a lock if possible. Ignore the record if it is expired.
            if (lockRecord)
            {
                var request = new PutAttributesRequest
                                  {
                                      DomainName = _domain,
                                      ItemName = BuildItemName(id),
                                      Expected = new UpdateCondition
                                                     {
                                                         Name = "Locked",
                                                         Value = Convert.ToString(false, Ic),
                                                     }
                                  };

                DateTimeOffset now = DateTimeOffset.UtcNow;

                Attr(request, "Locked", true);
                Attr(request, "LockDate", now);

                try
                {
                    _client.PutAttributes(request);

                    locked = false;

                    // we got the lock, check expires
                    var gar = new GetAttributesRequest
                                  {
                                      DomainName = _domain,
                                      ItemName = BuildItemName(id),
                                      ConsistentRead = true,
                                  };

                    gar.AttributeName.Add("Expires");

                    GetAttributesResponse result2 = _client.GetAttributes(gar);
                    List<Attribute> attr2 = result2.GetAttributesResult.Attribute;

                    expires = DateTimeOffset.Parse(attr2.First(x => x.Name == "Expires").Value, Ic);

                    if (expires < DateTimeOffset.UtcNow)
                    {
                        // oops the record is expired. handle it as locked.
                        locked = true;
                    }
                }
                catch (AmazonSimpleDBException e)
                {
                    if (e.StatusCode == HttpStatusCode.Conflict)
                    {
                        locked = true;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            // Retrieve the current session item information.

            var gr = new GetAttributesRequest
                         {
                             DomainName = _domain,
                             ItemName = BuildItemName(id),
                             ConsistentRead = true,
                         };

            gr.AttributeName.Add("Expires");
            gr.AttributeName.Add("SessionItems");
            gr.AttributeName.Add("LockId");
            gr.AttributeName.Add("LockDate");
            gr.AttributeName.Add("Flags");
            gr.AttributeName.Add("Timeout");

            GetAttributesResponse result = _client.GetAttributes(gr);
            List<Attribute> attr = result.GetAttributesResult.Attribute;

            expires = DateTimeOffset.Parse(attr.First(x => x.Name == "Expires").Value, Ic);

            if (expires < DateTimeOffset.UtcNow)
            {
                // The record was expired. Mark it as not locked.
                locked = false;
                // The session was expired. Mark the data for deletion.
                deleteData = true;
            }
            else
            {
                foundRecord = true;
            }

            serializedItems = attr.First(x => x.Name == "SessionItems").Value;
            lockId = attr.First(x => x.Name == "LockId").Value;
            lockAge =
                DateTimeOffset.UtcNow.Subtract(DateTimeOffset.Parse(attr.First(x => x.Name == "LockDate").Value, Ic));
            actionFlags = (SessionStateActions) Convert.ToInt32(attr.First(x => x.Name == "Flags").Value, Ic);
            timeout = Convert.ToInt32(attr.First(x => x.Name == "Timeout").Value, Ic);


            // If the returned session item is expired, 
            // delete the record from the data source.
            if (deleteData)
            {
                var del = new DeleteAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                              };

                _client.DeleteAttributes(del);
            }

            // The record was not found. Ensure that locked is false.
            if (!foundRecord)
                locked = false;

            // If the record was found and you obtained a lock, then set 
            // the lockId, clear the actionFlags,
            // and create the SessionStateStoreItem to return.
            if (foundRecord && !locked)
            {
                lockId = Convert.ToInt32(lockId, Ic) + 1;


                var request = new PutAttributesRequest
                                  {
                                      DomainName = _domain,
                                      ItemName = BuildItemName(id),
                                  };

                DateTimeOffset now = DateTimeOffset.UtcNow;

                Attr(request, "LockId", lockId);
                Attr(request, "Flags", 0);

                _client.PutAttributes(request);

                // If the actionFlags parameter is not InitializeItem, 
                // deserialize the stored SessionStateItemCollection.
                if (actionFlags == SessionStateActions.InitializeItem)
                    item = CreateNewStoreData(context, (int) _pConfig.Timeout.TotalMinutes);
                else
                    item = Deserialize(context, serializedItems, timeout);
            }

            return item;
        }


        public override void InitializeRequest(HttpContext context)
        {
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            var request = new PutAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                                  Expected = new UpdateCondition
                                                 {
                                                     Name = "LockId",
                                                     Value = Convert.ToString(lockId, Ic),
                                                 },
                              };

            DateTimeOffset now = DateTimeOffset.UtcNow;

            Attr(request, "Expires", now.AddMinutes(_pConfig.Timeout.TotalMinutes));
            Attr(request, "Locked", false);

            _client.PutAttributes(request);
        }

        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            var del = new DeleteAttributesRequest
                          {
                              DomainName = _domain,
                              ItemName = BuildItemName(id),
                              Expected = new UpdateCondition
                                             {
                                                 Name = "LockId",
                                                 Value = Convert.ToString(lockId, Ic),
                                             },
                          };

            _client.DeleteAttributes(del);
        }

        private string BuildItemName(string sessionId)
        {
            return _prefix + "-" + sessionId.Trim();
        }

        public override void ResetItemTimeout(HttpContext context, string id)
        {
            var request = new PutAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                              };

            Attr(request, "Timeout", _pConfig.Timeout.TotalMinutes);

            _client.PutAttributes(request);
        }

        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item,
                                                        object lockId, bool newItem)
        {
            // Serialize the SessionStateItemCollection as a string.
            string sessItems = Serialize((SessionStateItemCollection) item.Items);

            if (newItem)
            {
                // OdbcCommand to clear an existing expired session if it exists.

                const string query = @"select itemName() from {0} where itemName() = '{1}' and Expires < '{2}'";
                string ts = Convert.ToString(DateTimeOffset.UtcNow, Ic);

                var r = new SelectRequest
                            {
                                SelectExpression = String.Format(query, _domain, BuildItemName(id), ts),
                                ConsistentRead = true,
                            };

                DeleteableItem[] ids = _client.Select(r).SelectResult.Item
                    .Select(x => new DeleteableItem {ItemName = x.Name})
                    .ToArray();

                if (ids.Any())
                {
                    var dr = new BatchDeleteAttributesRequest
                                 {
                                     DomainName = _domain,
                                     Item = ids.ToList(),
                                 };

                    _client.BatchDeleteAttributes(dr);
                }


                // OdbcCommand to insert the new session item.
                var request = new PutAttributesRequest
                                  {
                                      DomainName = _domain,
                                      ItemName = BuildItemName(id),
                                  };

                DateTimeOffset now = DateTimeOffset.UtcNow;

                Attr(request, "Created", now);
                Attr(request, "Expires", now.AddMinutes(item.Timeout));
                Attr(request, "LockDate", now);
                Attr(request, "LockId", 0);
                Attr(request, "Timeout", item.Timeout);
                Attr(request, "Locked", false);
                Attr(request, "SessionItems", sessItems);
                Attr(request, "Flags", 0);

                _client.PutAttributes(request);
            }
            else
            {
                // OdbcCommand to update the existing session item.

                var request = new PutAttributesRequest
                                  {
                                      DomainName = _domain,
                                      ItemName = BuildItemName(id),
                                      Expected = new UpdateCondition
                                                     {
                                                         Name = "LockId",
                                                         Value = Convert.ToString(lockId, Ic),
                                                     }
                                  };

                DateTimeOffset now = DateTimeOffset.UtcNow;

                Attr(request, "Expires", now.AddMinutes(item.Timeout));
                Attr(request, "SessionItems", sessItems);
                Attr(request, "Locked", false);

                _client.PutAttributes(request);
            }
        }

        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

        public static string Serialize(SessionStateItemCollection items)
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);

            if (items != null)
                items.Serialize(writer);

            writer.Close();

            return Convert.ToBase64String(ms.ToArray());
        }

        public static SessionStateStoreData Deserialize(HttpContext context, string serializedItems, int timeout)
        {
            var ms = new MemoryStream(Convert.FromBase64String(serializedItems));

            var sessionItems =
                new SessionStateItemCollection();

            if (ms.Length > 0)
            {
                var reader = new BinaryReader(ms);
                sessionItems = SessionStateItemCollection.Deserialize(reader);
            }

            return new SessionStateStoreData(sessionItems,
                                             SessionStateUtility.GetSessionStaticObjects(context),
                                             timeout);
        }
    }
}