using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Dedup.Common;

namespace Dedup.Extensions
{
    public static class EnumExtensions
    {
        public static string GetDedupTypeDescription(this DedupType dedupType)
        {
            // get the field 
            var field = dedupType.GetType().GetField(dedupType.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return dedupType.ToString();
            }
        }
        public static string GetSimilarityTypeDescription(this SimilarityType methodType)
        {
            // get the field 
            var field = methodType.GetType().GetField(methodType.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return methodType.ToString();
            }
        }
        public static string GetArchiveRecordsDescription(this ArchiveRecords archiveRecords)
        {
            // get the field 
            var field = archiveRecords.GetType().GetField(archiveRecords.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return archiveRecords.ToString();
            }
        }
        public static string GetAuthGrantTypeDescription(this AuthGrantType authGrantType)
        {
            // get the field 
            var field = authGrantType.GetType().GetField(authGrantType.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return authGrantType.ToString();
            }
        }
        public static string GetSourceTypeDescription(this SourceType sourceType)
        {
            // get the field 
            var field = sourceType.GetType().GetField(sourceType.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return sourceType.ToString();
            }
        }
        public static string GetReviewBeforeDescription(this ReviewBeforeDeleteDups review)
        {
            // get the field 
            var field = review.GetType().GetField(review.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return review.ToString();
            }
        }

        public static string GetDedupMatchDescription(this DedupMatchType dmType)
        {
            // get the field 
            var field = dmType.GetType().GetField(dmType.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return dmType.ToString();
            }
        }
    }
}
