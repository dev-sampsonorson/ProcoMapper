using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Data.SqlClient;
using System.Data;
using System.Collections.Concurrent;
using System.Data.Common;

namespace Blendy.Core.DALayer.EntityMapper {
    
    /// <summary>
    /// SqlHelperParameterCache provides functions to leverage a static cache of procedure parameters, and the
    /// ability to discover parameters for stored procedures at run-time.
    /// </summary>
    public sealed class SqlHelperParameterCache {

        private static ConcurrentDictionary<string, IList<IDataParameter>> paramCache = new ConcurrentDictionary<string, IList<IDataParameter>>();
        //private static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());

        private static IList<IDataParameter> DiscoverSpParameterSet(IDbConnection connection, string spName, bool includeReturnValueParameter) {
            if (connection == null) throw new ArgumentNullException("connection");
            if (spName == null || spName.Length == 0) throw new ArgumentNullException("spName");

            using (IDbCommand command = connection.CreateCommand()) {
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = spName;
                SqlCommandBuilder.DeriveParameters((SqlCommand)command);

                if (!includeReturnValueParameter) {
                    command.Parameters.RemoveAt(0);
                }

                IList<IDataParameter> discoveredParameters = new List<IDataParameter>(command.Parameters.OfType<IDataParameter>()); //new SqlParameter[command.Parameters.Count];

                //command.Parameters.CopyTo(discoveredParameters, 0);

                //command.Parameters.Clear();

                // Init the parameters with a DBNull value
                foreach (IDataParameter discoveredParameter in discoveredParameters) {
                    if (discoveredParameter.Direction == ParameterDirection.InputOutput)
                        discoveredParameter.Direction = ParameterDirection.Output;

                    discoveredParameter.Value = DBNull.Value;
                }

                return discoveredParameters;
            }
        }

        /// <summary>
        /// Retrieves the set of SqlParameters appropriate for the stored procedure
        /// </summary>
        /// <param name="connection">A valid SqlConnection object</param>
        /// <param name="spName">The name of the stored procedure</param>
        /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
        /// <returns>An array of SqlParameters</returns>
        public static IList<IDataParameter> GetSpParameterSet(IDbConnection connection, string spName, bool includeReturnValueParameter) {
            if (connection == null) throw new ArgumentNullException("connection");
            if (spName == null || spName.Length == 0) throw new ArgumentNullException("spName");

            string hashKey = connection.ConnectionString + ":" + spName + (includeReturnValueParameter ? ":include ReturnValue Parameter" : "");

            IList<IDataParameter> cachedParameters = null;
            if (!paramCache.TryGetValue(hashKey, out cachedParameters)) {
                cachedParameters = DiscoverSpParameterSet(connection, spName, includeReturnValueParameter);
                paramCache[hashKey] = cachedParameters;
            }

            return CloneParameters(cachedParameters);
        }

        /// <summary>
        /// Deep copy of cached SqlParameter array
        /// </summary>
        /// <param name="originalParameters"></param>
        /// <returns></returns>
        private static IList<IDataParameter> CloneParameters(IList<IDataParameter> originalParameters) {
            if (originalParameters == null) throw new ArgumentNullException("CloneParameters: originalParameters");

            IList<IDataParameter> clonedParameters = new List<IDataParameter>();

            foreach (IDataParameter p in originalParameters) {
                clonedParameters.Add((IDataParameter)((ICloneable)p).Clone());
            }


            return clonedParameters;
        }

    }
}
