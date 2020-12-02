using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Blendy.Core.DALayer.EntityMapper {
    public static class ObjectExtension {

        /// <summary>
        /// Casts an object of one type to an object of 
        /// another type using reflection
        /// 
        /// An OverflowException if the cast fails. 
        /// Eg. Value was either too large or too small for an unsigned byte
        /// </summary>
        /// <param name="target">The object to cast</param>
        /// <param name="to">The type to cast the object to</param>
        /// <returns>The casted value is returned</returns>
        public static object CastTo(this object target, Type to) {
            try {
                Type targetType = target.GetType().IsNullable() ? Nullable.GetUnderlyingType(target.GetType()) : target.GetType();
                var convertedValue = target == null ? null : Convert.ChangeType(target, to);

                return convertedValue;
            } catch (Exception ex) {
                throw ex;
            }

        }
    }
}
