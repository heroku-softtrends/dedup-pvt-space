using RestSharp;
using System;

namespace Dedup.Common
{
    public class RequestFactory : IRequestFactory
    {
        /// <summary>
        /// Returns new REST client instance.
        /// </summary>
        public IRestClient CreateClient()
        {
            return new RestClient();
        }

        /// <summary>
        /// Returns new REST client instance
        /// </summary>
        /// <param name="timeOut"></param>
        /// <param name="baseUrl"></param>
        /// <returns></returns>
        public IRestClient CreateHerokuRestClient(string baseUrl, int timeOut = Constants.RESTCLIENT_TIMEOUT)
        {
            return new RestClient()
            {
                Timeout = timeOut,
                BaseUrl = new Uri(baseUrl)
            };
        }

        /// <summary>
        /// Returns new REST client instance for heroku api call
        /// </summary>
        /// <param name="baseUrl"></param>
        /// <param name="timeOut"></param>
        /// <returns></returns>
        public IRestClient CreateHerokuRestClient(int timeOut = Constants.RESTCLIENT_TIMEOUT)
        {
            return new RestClient()
            {
                Timeout = timeOut,
                BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl)
            };
        }

        /// <summary>
        /// Returns new REST request instance.
        /// </summary>
        public IRestRequest CreateRequest()
        {
            return new RestRequest();
        }
    }
}
