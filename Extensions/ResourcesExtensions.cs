using System;
using Dedup.Models;
using Dedup.ViewModels;

namespace Dedup.Extensions
{
    public static class ResourcesExtensions
    {
        public static Resources ToResources(this ResourceViewModel resource)
        {
            if (resource.IsNull() || (resource.IsNull() == false && resource.uuid == null))
                return null;

            return new Resources()
            {
                uuid = resource.uuid.ToString(),
                heroku_id = resource.heroku_id,
                plan = resource.plan,
                callback_url = resource.callback_url,
                region = resource.region
            };
        }

        public static ResourceViewModel ToResource(this Resources resources)
        {
            if (resources == null || (resources != null && string.IsNullOrEmpty(resources.uuid)))
                return default(ResourceViewModel);

            return new ResourceViewModel()
            {
                uuid = new Guid(resources.uuid),
                heroku_id = resources.heroku_id,
                plan = resources.plan,
                callback_url = resources.callback_url,
                region = resources.region,
                created_at = resources.created_at,
                updated_at = resources.updated_at,
                app_name = (string.IsNullOrEmpty(resources.app_name) ? resources.heroku_id : resources.app_name)
            };
        }
    }
}
