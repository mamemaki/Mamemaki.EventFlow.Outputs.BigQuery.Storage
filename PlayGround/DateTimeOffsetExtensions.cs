using System;

namespace PlayGround
{
    internal static class DateTimeOffsetExtensions
    {
        private static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>
        /// <see cref="https://stackoverflow.com/a/70024396/415694"/>
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public static long ToUnixTimeMicroseconds(this DateTimeOffset timestamp)
        {
            TimeSpan duration = timestamp - UnixEpoch;
            // There are 10 ticks per microsecond.
            return duration.Ticks / 10;
        }
    }
}
