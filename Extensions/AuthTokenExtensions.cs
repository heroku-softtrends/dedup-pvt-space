using Dedup.Models;
using Dedup.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Extensions
{
    public static class AuthTokenExtensions
    {
        public static HerokuAuthToken ToHerokuToken(this AuthTokens authToken)
        {
            if (authToken == null)
                return default(HerokuAuthToken);

            return new HerokuAuthToken
            {
                auth_id = authToken.auth_id,
                access_token = authToken.access_token,
                expires_in = (int)TimeSpan.FromTicks(DateTime.Now.Ticks).TotalSeconds,
                refresh_token = authToken.refresh_token,
                token_type = authToken.token_type,
                user_id = authToken.user_id,
                session_nonce = authToken.session_nonce
            };
        }

        public static AuthTokens ToAuthToken(this HerokuAuthToken authToken)
        {
            if (authToken.IsNull())
                return default(AuthTokens);

            return new AuthTokens
            {
                auth_id = authToken.auth_id,
                access_token = authToken.access_token,
                expires_in = DateTime.Now.AddSeconds(authToken.expires_in - 120),
                refresh_token = authToken.refresh_token,
                token_type = authToken.token_type,
                user_id = authToken.user_id,
                session_nonce = authToken.session_nonce
            };
        }
        public static PartnerAuthTokens ToPartnerAuthToken(this OauthGrant oauthGrant, string authId)
        {
            if (oauthGrant.IsNull())
                return default(PartnerAuthTokens);

            return new PartnerAuthTokens
            {
                auth_id = authId,
                oauth_code = oauthGrant.code,
                oauth_type = oauthGrant.type,
                oauth_expired_in = DateTime.Now.AddSeconds(280)
            };
        }
        public static PartnerAuthTokens ToPartnerAuthToken(this HerokuAuthToken authToken)
        {
            if (authToken.IsNull())
                return default(PartnerAuthTokens);

            return new PartnerAuthTokens
            {
                auth_id = authToken.auth_id,
                access_token = authToken.access_token,
                expires_in = DateTime.Now.AddSeconds(authToken.expires_in - 120),
                refresh_token = authToken.refresh_token,
                token_type = authToken.token_type,
                user_id = authToken.user_id,
                session_nonce = authToken.session_nonce
            };
        }
    }
}
