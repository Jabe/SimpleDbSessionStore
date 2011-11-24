using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
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
        private bool _compress;
        private SessionStateSection _configSection;
        private string _domain;
        private string _key;
        private string _prefix;
        private string _secret;
        private string _serviceUrl;

        public override void Initialize(string name, NameValueCollection config)
        {
            if (config == null) throw new ArgumentNullException("config");

            if (string.IsNullOrEmpty(name))
            {
                name = "SdbSessionStateStore";
            }

            if (string.IsNullOrEmpty(config["description"]))
            {
                config["description"] = "SimpleDB Session Provider";
            }

            base.Initialize(name, config);

            Configuration cfg = WebConfigurationManager.OpenWebConfiguration(HostingEnvironment.ApplicationVirtualPath);
            _configSection = (SessionStateSection) cfg.GetSection("system.web/sessionState");

            _compress = _configSection.CompressionEnabled;

            _key = config["key"];
            _secret = config["secret"];
            _domain = config["domain"];
            _prefix = config["prefix"];
            _serviceUrl = config["serviceUrl"];

            if (string.IsNullOrEmpty(_key)) throw new ArgumentException("key");
            if (string.IsNullOrEmpty(_secret)) throw new ArgumentException("secret");
            if (string.IsNullOrEmpty(_domain)) throw new ArgumentException("domain");
            if (string.IsNullOrEmpty(_prefix)) throw new ArgumentException("prefix");

            var sdbc = new AmazonSimpleDBConfig();

            if (!string.IsNullOrEmpty(_serviceUrl))
            {
                sdbc.ServiceURL = _serviceUrl;
            }

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

        public override void Dispose()
        {
            if (_client != null)
            {
                _client.Dispose();
                _client = null;
            }
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

        public override void InitializeRequest(HttpContext context)
        {
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            var expected = new UpdateCondition
                               {
                                   Name = "LockId",
                                   Value = ToString(lockId)
                               };

            var request = new PutAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                                  Expected = expected
                              };

            DateTimeOffset now = DateTimeOffset.UtcNow;

            Attr(request, "Expires", now.AddMinutes(_configSection.Timeout.TotalMinutes));
            Attr(request, "Locked", false);

            try
            {
                _client.PutAttributes(request);
            }
            catch (AmazonSimpleDBException e)
            {
                if (e.StatusCode == HttpStatusCode.Conflict)
                {
                    // lock failed. nothing to release. let's just walk away...
                    return;
                }

                throw;
            }
        }

        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            var expected = new UpdateCondition
                               {
                                   Name = "LockId",
                                   Value = ToString(lockId)
                               };

            var request = new DeleteAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                                  Expected = expected
                              };

            try
            {
                _client.DeleteAttributes(request);
            }
            catch (AmazonSimpleDBException e)
            {
                if (e.StatusCode == HttpStatusCode.Conflict)
                {
                    // lock failed. back off!
                    return;
                }

                throw;
            }
        }

        public override void ResetItemTimeout(HttpContext context, string id)
        {
            var request = new PutAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id)
                              };

            Attr(request, "Timeout", _configSection.Timeout.TotalMinutes);

            _client.PutAttributes(request);
        }

        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item,
                                                        object lockId, bool newItem)
        {
            string sessItems = Serialize(item.Items as SessionStateItemCollection);

            if (newItem)
            {
                // write a new session
                // the sample provider had some code here to clean up a potentially expired session.
                // well. we just overwrite the session completely.

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
                Attr(request, "Flags", 0);

                foreach (string value in SplitStringWithIndex(sessItems, 1000))
                {
                    Attr(request, "SessionItems", value);
                }

                _client.PutAttributes(request);
            }
            else
            {
                // update the existing session

                var expected = new UpdateCondition
                                   {
                                       Name = "LockId",
                                       Value = ToString(lockId)
                                   };

                var request = new PutAttributesRequest
                                  {
                                      DomainName = _domain,
                                      ItemName = BuildItemName(id),
                                      Expected = expected
                                  };

                DateTimeOffset now = DateTimeOffset.UtcNow;

                Attr(request, "Expires", now.AddMinutes(item.Timeout));
                Attr(request, "Locked", false);

                foreach (string value in SplitStringWithIndex(sessItems, 1000))
                {
                    Attr(request, "SessionItems", value);
                }

                try
                {
                    _client.PutAttributes(request);
                }
                catch (AmazonSimpleDBException e)
                {
                    if (e.StatusCode == HttpStatusCode.Conflict)
                    {
                        // lock failed. I think it would be best to back-off.
                        return;
                    }

                    throw;
                }
            }
        }

        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

        private SessionStateStoreData GetSessionStoreItem(bool lockRecord, HttpContext context, string id,
                                                          out bool locked, out TimeSpan lockAge, out object lockId,
                                                          out SessionStateActions actionFlags)
        {
            // GetSessionStoreItem is called by both the GetItem and 
            // GetItemExclusive methods. GetSessionStoreItem retrieves the 
            // session data from the data source. If the lockRecord parameter
            // is true (in the case of GetItemExclusive), then GetSessionStoreItem
            // locks the record and sets a new LockId and LockDate.

            // Initial values for return value and out parameters.
            SessionStateStoreData item = null;
            lockAge = TimeSpan.Zero;
            lockId = null;
            locked = false;
            actionFlags = 0;

            // String to hold serialized SessionStateItemCollection.
            string serializedItems = "";
            // True if a record is found in the database.
            bool found = true;
            // Timeout value from the data store.
            int timeout = 0;

            // lockRecord is true when called from GetItemExclusive and
            // false when called from GetItem.
            // Obtain a lock if possible. Ignore the record if it is expired.
            
            if (lockRecord)
            {
                TryAcquireLock(id, out locked, out found);
            }

            DateTimeOffset now = DateTimeOffset.UtcNow;

            // shortcut if record wasnt found while locking
            if (found)
            {
                found = false;

                List<Attribute> attributes = LoadSession(id);

                // valid looking session?
                // Timeout is most likly to be set... so look for > 1
                // after we return, CreateUninitializedItem will be called, then we again.
                if (attributes.Count > 1)
                {
                    try
                    {
                        var expires = FromString<DateTimeOffset>(FirstVal("Expires", attributes));

                        if (expires > now)
                        {
                            lockId = FromString<int>(FirstVal("LockId", attributes));
                            lockAge = now.Subtract(FromString<DateTimeOffset>(FirstVal("LockDate", attributes)));
                            actionFlags = (SessionStateActions) FromString<int>(FirstVal("Flags", attributes));
                            timeout = FromString<int>(FirstVal("Timeout", attributes));

                            IEnumerable<string> chunks = attributes
                                .Where(x => x.Name == "SessionItems")
                                .Select(x => x.Value);

                            serializedItems = UnSplitStringWithIndex(chunks);

                            found = true;
                        }
                        else
                        {
                            // expired
                            // TODO: janitor for cleanup. 
                            locked = false;
                        }
                    }
                    catch
                    {
                        // invalid session data
                        locked = false;
                    }
                }
                else
                {
                    locked = false;
                }
            }

            // If the record was found and you obtained a lock, then set 
            // the lockId, clear the actionFlags,
            // and create the SessionStateStoreItem to return.
            if (found && !locked)
            {
                lockId = (int) lockId + 1;

                var request = new PutAttributesRequest
                                  {
                                      DomainName = _domain,
                                      ItemName = BuildItemName(id),
                                  };

                Attr(request, "LockId", lockId);
                Attr(request, "Flags", 0);

                _client.PutAttributes(request);

                // If the actionFlags parameter is not InitializeItem, 
                // deserialize the stored SessionStateItemCollection.
                if (actionFlags == SessionStateActions.InitializeItem)
                {
                    item = CreateNewStoreData(context, (int) _configSection.Timeout.TotalMinutes);
                }
                else
                {
                    item = Deserialize(context, serializedItems, timeout);
                }
            }

            // item == null leads to CreateUninitializedItem

            return item;
        }

        private void TryAcquireLock(string id, out bool locked, out bool found)
        {
            DateTimeOffset now = DateTimeOffset.Now;

            var expected = new UpdateCondition
                               {
                                   Name = "Locked",
                                   Value = ToString(false)
                               };

            var request = new PutAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                                  Expected = expected
                              };

            Attr(request, "Locked", true);
            Attr(request, "LockDate", now);

            // best case
            locked = false;
            found = true;

            try
            {
                _client.PutAttributes(request);
            }
            catch (AmazonSimpleDBException e)
            {
                if (e.StatusCode == HttpStatusCode.Conflict)
                {
                    // couldnt obtain the lock
                    locked = true;
                }
                else if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    // "Locked" attr failed.
                    found = false;
                }
                else
                {
                    throw;
                }
            }
        }

        private List<Attribute> LoadSession(string id)
        {
            var request = new GetAttributesRequest
                              {
                                  DomainName = _domain,
                                  ItemName = BuildItemName(id),
                                  ConsistentRead = true
                              };

            request.AttributeName.Add("Expires");
            request.AttributeName.Add("SessionItems");
            request.AttributeName.Add("LockId");
            request.AttributeName.Add("LockDate");
            request.AttributeName.Add("Flags");
            request.AttributeName.Add("Timeout");

            GetAttributesResponse result = _client.GetAttributes(request);

            return result.GetAttributesResult.Attribute;
        }

        private string Serialize(SessionStateItemCollection items)
        {
            if (items == null) return ""; // ascii85 of empty byte[]

            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                items.Serialize(writer);

                // finish serialization
                writer.Flush();

                // final uncompressed length
                var length = (int) ms.Length;

                // get 'the' buffer
                byte[] buffer = ms.GetBuffer();

                if (_compress)
                {
                    // this wont clear the buffer (data will be overwritten)
                    ms.SetLength(0);

                    // create a deflate stream with the memory stream as target.
                    using (var compressor = new DeflateStream(ms, CompressionMode.Compress, true))
                    {
                        // write all data from 'the' buffer to 'the' buffer. :-)
                        compressor.Write(buffer, 0, length);
                    }

                    // final compressed length
                    length = (int) ms.Length;
                }

                return GetAscii85().Encode(buffer, 0, length);
            }
        }

        private SessionStateStoreData Deserialize(HttpContext context, string serializedItems, int timeout)
        {
            HttpStaticObjectsCollection staticObjects = SessionStateUtility.GetSessionStaticObjects(context);

            byte[] bytes = GetAscii85().Decode(serializedItems);

            SessionStateItemCollection sessionItems;

            if (bytes.Length > 0)
            {
                using (var ms = new MemoryStream(bytes))
                {
                    Stream target = ms;

                    if (_compress)
                    {
                        target = new DeflateStream(target, CompressionMode.Decompress);
                    }

                    using (target)
                    using (var reader = new BinaryReader(target))
                    {
                        sessionItems = SessionStateItemCollection.Deserialize(reader);
                    }
                }
            }
            else
            {
                sessionItems = new SessionStateItemCollection();
            }

            return new SessionStateStoreData(sessionItems, staticObjects, timeout);
        }

        private string BuildItemName(string sessionId)
        {
            return _prefix + "-" + sessionId.Trim();
        }

        private static void Attr(PutAttributesRequest request, string name, object value, bool replace = true)
        {
            request.Attribute.Add(new ReplaceableAttribute {Name = name, Replace = replace, Value = ToString(value)});
        }

        private static string ToString(object value)
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

            return str;
        }

        private static T FromString<T>(string str)
        {
            object val;

            if (typeof (T) == typeof (DateTimeOffset))
            {
                val = DateTimeOffset.ParseExact(str, "o", Ic);
            }
            else if (typeof (T) == typeof (bool))
            {
                val = Convert.ToBoolean(str, Ic);
            }
            else if (typeof (T) == typeof (int))
            {
                val = Convert.ToInt32(str, Ic);
            }
            else
            {
                throw new Exception("Unsupported type: " + typeof (T).Name);
            }

            return (T) val;
        }

        private static IEnumerable<string> SplitStringWithIndex(string str, int chunkSize)
        {
            return SplitString(str, chunkSize).Select((s, i) => i.ToString("X2") + s);
        }

        private static IEnumerable<string> SplitString(string str, int chunkSize)
        {
            int bytes = Encoding.UTF8.GetByteCount(str);

            // shortcut
            if (bytes < chunkSize)
            {
                return new[] {str};
            }

            var chunks = new List<string>();

            Encoder encoder = Encoding.UTF8.GetEncoder();

            var outputBytes = new byte[chunkSize];
            char[] input = str.ToCharArray();

            bool completed = false;

            for (int i = 0; !completed;)
            {
                int bytesUsed;
                int charsUsed;

                encoder.Convert(input, i, input.Length - i, outputBytes, 0, outputBytes.Length, true, out charsUsed,
                                out bytesUsed, out completed);

                chunks.Add(Encoding.UTF8.GetString(outputBytes, 0, bytesUsed));

                i += charsUsed;
            }

            return chunks;
        }

        private string UnSplitStringWithIndex(IEnumerable<string> chunks)
        {
            string str = "";

            foreach (string s in chunks.OrderBy(x => x))
            {
                if (s != "")
                {
                    str += s.Substring(2);
                }
            }

            return str;
        }

        private static Ascii85 GetAscii85()
        {
            return new Ascii85 {EnforceMarks = false, LineLength = 0};
        }

        private static string FirstVal(string s, IEnumerable<Attribute> attributes)
        {
            return attributes.First(x => x.Name == s).Value;
        }
    }
}