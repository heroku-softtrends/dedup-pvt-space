using RestSharp;

namespace Dedup.Common
{
    public interface IRequestFactory
    {
        /// <summary>
        /// Returns new REST client instance.
        /// </summary>
        IRestClient CreateClient();

        /// <summary>
        /// Returns new REST request instance.
        /// </summary>
        IRestRequest CreateRequest();


        /// <summary>
        /// Returns new REST client instance.
        /// </summary>
        IRestClient CreateHerokuRestClient(string baseUrl, int timeOut = Constants.RESTCLIENT_TIMEOUT);

        /// <summary>
        /// Returns new REST client instance.
        /// </summary>
        IRestClient CreateHerokuRestClient(int timeOut = Constants.RESTCLIENT_TIMEOUT);

    }
}
