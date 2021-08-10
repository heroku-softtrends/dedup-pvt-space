using Hangfire;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Common
{
    public static class DedupCron
    {
        public static string Minutely()
        {
            return "* * * * *";
        }

        public static string Hourly()
        {
            return Cron.Hourly(0);
        }

        public static string Hourly(int minute)
        {
            return string.Format("{0} * * * *", minute);
        }

        public static string Daily()
        {
            return Cron.Daily(0);
        }

        public static string Daily(int hour)
        {
            return Cron.Daily(hour, 0);
        }

        public static string Daily(int hour, int minute)
        {
            return string.Format("{0} {1} * * *", minute, hour);
        }

        public static string Weekly()
        {
            return Cron.Weekly();
        }

        public static string Weekly(DayOfWeek dayOfWeek)
        {
            return Cron.Weekly(dayOfWeek, 0);
        }

        public static string Weekly(DayOfWeek dayOfWeek, int hour)
        {
            return Cron.Weekly(dayOfWeek, hour, 0);
        }

        public static string Weekly(DayOfWeek dayOfWeek, int hour, int minute)
        {
            return string.Format("{0} {1} * * {2}", minute, hour, dayOfWeek);
        }

        public static string Monthly()
        {
            return Cron.Monthly(1);
        }

        public static string Monthly(int day)
        {
            return Cron.Monthly(day, 0);
        }

        public static string Monthly(int day, int hour)
        {
            return Cron.Monthly(day, hour, 0);
        }

        public static string Monthly(int day, int hour, int minute)
        {
            return string.Format("{0} {1} {2} * *", minute, hour, day);
        }

        public static string Yearly()
        {
            return Cron.Yearly(1);
        }

        public static string Yearly(int month)
        {
            return Cron.Yearly(month, 1);
        }

        public static string Yearly(int month, int day)
        {
            return Cron.Yearly(month, day, 0);
        }

        public static string Yearly(int month, int day, int hour)
        {
            return Cron.Yearly(month, day, hour, 0);
        }

        public static string Yearly(int month, int day, int hour, int minute)
        {
            return string.Format("{0} {1} {2} {3} *", new object[]
            {
                minute,
                hour,
                day,
                month
            });
        }

        public static string MinuteInterval(int interval)
        {
            if (interval > 59)
            {
                if (interval % 60 > 0)
                    return string.Format("{0} */{1} * * *", interval % 60, (int)Math.Floor((double)interval / 60));
                else
                    return string.Format("0 */{0} * * *", (int)Math.Floor((double)interval / 60));
            }
            else
            {
                return string.Format("*/{0} * * * *", interval);
            }
        }

        public static string HourInterval(int interval)
        {
            return string.Format("0 */{0} * * *", interval);
        }

        public static string DayInterval(int interval)
        {
            return string.Format("0 0 */{0} * *", interval);
        }

        public static string MonthInterval(int interval)
        {
            return string.Format("0 0 1 */{0} *", interval);
        }
    }
}
