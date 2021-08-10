using RestSharp;
using System.Threading.Tasks;

namespace Dedup.Extensions
{
    public static class RestClientExtension
    {
        public static Task<IRestResponse> RestExecuteAsync(this IRestClient client, IRestRequest request)
        {
            var tcs = new TaskCompletionSource<IRestResponse>();

            client.ExecuteAsync(request, response =>
            {
                tcs.SetResult(response);
            });

            return tcs.Task;
        }
    }
}
