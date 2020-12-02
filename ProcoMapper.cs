#region Comments

/* 
 * ASP.NET 4.0: ProcoMapper
 * 
 * All rights reserved.
 * THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY
 * OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR
 * FITNESS FOR A PARTICULAR PURPOSE.
 * 
 * Author: Sampson Orson Jackson
 * Description: Use this class to retreive and persist data to the database.
 * Email: sampson.orson@gmail.com
 *  
 * Release history
 * Date         Initials    Description
 * 19/11/2012   S.O.J       Initial Version
 * 
*/

#endregion Comments

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Data;
using System.Data.SqlClient;
using System.Reflection.Emit;
using System.Collections.Concurrent;
using System.Threading;
using System.Collections;
using System.Linq;
using System.ComponentModel;
using System.Dynamic;
using Blendy.Core.DALayer.DbGateway;

namespace Blendy.Core.DALayer.EntityMapper {

    /// <summary>
    /// Use this class to retreive and persist data to the database
    /// </summary>
    public class ProcoMapper {

        #region Delegates

        public delegate IDbCommand AttachParamToCommand(IDbCommand command, List<IDataParameter> parameters, object entity, ref IPartialParam partialParam); //object => TEntity
        public delegate object EntityDeserializer(IDataReader dr, bool implicitCast, dynamic partialParam);

        #endregion

        #region Cache EntityDeserializer

        public static readonly ConcurrentDictionary<string, EntityDeserializer> _entityDeserializer = new ConcurrentDictionary<string, EntityDeserializer>();
        public static void SetQueryCache(string key, EntityDeserializer value) {
            _entityDeserializer[key] = value;
        }
        public static bool TryGetQueryCache(string key, out EntityDeserializer value) {
            if (_entityDeserializer.TryGetValue(key, out value)) {
                return true;
            }
            value = null;
            return false;
        }

        #endregion

        #region Properties

        private static readonly MethodInfo getItem = typeof(IDataRecord).GetProperties(BindingFlags.Instance | BindingFlags.Public)
                        .Where(p => p.GetIndexParameters().Any() && p.GetIndexParameters()[0].ParameterType == typeof(int))
                        .Select(p => p.GetGetMethod()).First();

        #endregion

        #region Private Methods

        private static bool HasPartialProperty(ExpandoObject expando, string key) {
            return ((IDictionary<string, object>)expando).ContainsKey(key);
        }

        private static void EmitInt32(ILGenerator il, int value) {
            switch (value) {
                case -1: il.Emit(OpCodes.Ldc_I4_M1); break;
                case 0: il.Emit(OpCodes.Ldc_I4_0); break;
                case 1: il.Emit(OpCodes.Ldc_I4_1); break;
                case 2: il.Emit(OpCodes.Ldc_I4_2); break;
                case 3: il.Emit(OpCodes.Ldc_I4_3); break;
                case 4: il.Emit(OpCodes.Ldc_I4_4); break;
                case 5: il.Emit(OpCodes.Ldc_I4_5); break;
                case 6: il.Emit(OpCodes.Ldc_I4_6); break;
                case 7: il.Emit(OpCodes.Ldc_I4_7); break;
                case 8: il.Emit(OpCodes.Ldc_I4_8); break;
                default:
                    if (value >= -128 && value <= 127) {
                        il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                    } else {
                        il.Emit(OpCodes.Ldc_I4, value);
                    }
                    break;
            }
        }

        private static AttachParamToCommand GenerateAttachParamToCommand<TEntity>(IDbCommand command, List<IDataParameter> paramList,
            ref IPartialParam partialParam) where TEntity : class, new() {
            //Func<IDbCommand, List<IDataParameter>, object, IPartialParam, IDbCommand>
            //AttachParamToCommand(IDbCommand command, List<IDataParameter> parameters, object entity, ref IPartialParam partialParam

            DynamicMethod dm = new DynamicMethod("AttachParamToCommand" + Guid.NewGuid().ToString(),
                MethodAttributes.Public | MethodAttributes.Static, CallingConventions.Standard,
                typeof(IDbCommand), new Type[] { typeof(IDbCommand), typeof(List<IDataParameter>), typeof(object),
                    Type.GetType(typeof(ProcoMapper).Namespace + ".IPartialParam&") },
                    typeof(ProcoMapper), false); //
            //MethodBuilder factory = dm.MakeGenericMethod(

            ILGenerator il = dm.GetILGenerator();

            Type paramListType = paramList.GetType();

            Label lblHasMoreParams = il.DefineLabel();
            Label lblLoadValueAgain = il.DefineLabel();
            Label lblIncreament = il.DefineLabel();
            Label lblAddParamToCommand = il.DefineLabel();
            Label lblParmIsNullFindInEntity = il.DefineLabel();
            Label lblReturn = il.DefineLabel();
            Label lblParamIsInput = il.DefineLabel();
            Label lblHasSetParamValueCond = il.DefineLabel();
            Label lblHasSetParamValueCond2 = il.DefineLabel();

            il.DeclareLocal(typeof(int)); //0 => index
            il.DeclareLocal(typeof(bool)); //1 => index++
            il.DeclareLocal(typeof(IDataParameter)); //2 => p
            il.DeclareLocal(typeof(bool)); //3 => hasSetParamValue
            il.DeclareLocal(typeof(IPartialParamInfo)); //4 => param
            il.DeclareLocal(typeof(string)); //5 => paramName
            il.DeclareLocal(typeof(char[])); //6 = char[] 
            il.DeclareLocal(typeof(PropertyInfo)); //7 => prop
            il.DeclareLocal(typeof(IDbCommand)); //8 => command           

            il.Emit(OpCodes.Ldc_I4_0); //Init the variable index
            il.Emit(OpCodes.Stloc_0);
            il.Emit(OpCodes.Br, lblHasMoreParams);

            //It must have returned true
            //------------------------------------------------------------------------------
            il.MarkLabel(lblLoadValueAgain);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Callvirt, paramListType.GetMethod("get_Item", new Type[] { typeof(int) }));
            il.Emit(OpCodes.Stloc_2);
            il.Emit(OpCodes.Ldloc_2);
            il.Emit(OpCodes.Callvirt, typeof(IDataParameter).GetMethod("get_Direction"));
            il.Emit(OpCodes.Ldc_I4_6);
            il.Emit(OpCodes.Ceq);
            il.Emit(OpCodes.Stloc_1);
            il.Emit(OpCodes.Ldloc_1);

            il.Emit(OpCodes.Brtrue, lblAddParamToCommand); //if (p.Direction != ParameterDirection.ReturnValue) GOTO lblAddParamToCommand
            //if (p.Direction != ParameterDirection.ReturnValue) {
            //bool hasSetParamValue = false; //4 => hasSetParamValue
            //IPartialParamInfo param = null; //5 => param
            //string paramName = p.ParameterName.TrimStart('@'); //6 => paramName //7 = char[]
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stloc_3);
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Stloc_S, 4);
            il.Emit(OpCodes.Ldloc_2);
            il.Emit(OpCodes.Callvirt, typeof(IDataParameter).GetMethod("get_ParameterName"));  //IDataParameter::get_ParameterName()
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Newarr, typeof(Char));
            il.Emit(OpCodes.Stloc_S, 6);
            il.Emit(OpCodes.Ldloc_S, 6);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Ldc_I4_S, 64);
            il.Emit(OpCodes.Stelem_I2);
            il.Emit(OpCodes.Ldloc_S, 6); //Load unto stack
            il.Emit(OpCodes.Callvirt, typeof(string).GetMethod("TrimStart", new Type[] { typeof(char[]) }));
            il.Emit(OpCodes.Stloc_S, 5);

            //Check in the partial pram
            //param = partialParam.Get(paramName);
            il.Emit(OpCodes.Ldarg_3);
            il.Emit(OpCodes.Ldind_Ref);
            il.Emit(OpCodes.Ldloc_S, 5); //paramName
            il.Emit(OpCodes.Callvirt, typeof(IPartialParam).GetMethod("Get", new Type[] { typeof(string) }));
            il.Emit(OpCodes.Stloc_S, 4);  //param 

            //if (param != null) {
            il.Emit(OpCodes.Ldloc_S, 4);
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Ceq);
            il.Emit(OpCodes.Stloc_1);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Brtrue, lblParmIsNullFindInEntity);

            //p.Value = param.GetValue(); 
            il.Emit(OpCodes.Ldloc_2); //p
            il.Emit(OpCodes.Ldloc_S, 4); //param
            il.Emit(OpCodes.Callvirt, typeof(IPartialParamInfo).GetMethod("GetValue"));
            il.Emit(OpCodes.Callvirt, typeof(IDataParameter).GetMethod("set_Value", new Type[] { typeof(object) }));

            //hasSetParamValue = true;
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Stloc_3);
            il.Emit(OpCodes.Br, lblParamIsInput);
            //} 
            //else { => if (param != null) {
            il.MarkLabel(lblParmIsNullFindInEntity);


            //PropertyInfo prop = typeof(TEntity).GetProperty(paramName); 
            il.Emit(OpCodes.Ldtoken, typeof(TEntity));
            il.Emit(OpCodes.Call, typeof(Type).GetMethod("GetTypeFromHandle", new Type[] { typeof(RuntimeTypeHandle) }));
            il.Emit(OpCodes.Ldloc_S, 5);
            il.Emit(OpCodes.Call, typeof(Type).GetMethod("GetProperty", new Type[] { typeof(string) }));
            il.Emit(OpCodes.Stloc_S, 7);


            //if (prop != null) {
            il.Emit(OpCodes.Ldloc_S, 7);
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Call, typeof(PropertyInfo).GetMethod("op_Inequality", new Type[] { typeof(PropertyInfo), typeof(PropertyInfo) }));
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Ceq);
            il.Emit(OpCodes.Stloc_1);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Brtrue, lblParamIsInput);

            //p.Value = prop.GetValue(entity, null); 
            il.Emit(OpCodes.Ldloc_2);
            il.Emit(OpCodes.Ldloc_S, 7); //prop
            il.Emit(OpCodes.Ldarg_2);
            il.Emit(OpCodes.Box, typeof(TEntity));
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Callvirt, typeof(PropertyInfo).GetMethod("GetValue", new Type[] { typeof(TEntity), typeof(object[]) }));
            il.Emit(OpCodes.Callvirt, typeof(IDataParameter).GetMethod("set_Value", new Type[] { typeof(object) }));

            //hasSetParamValue = true;
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Stloc_3); //hasSetParamValue

            //}

            //if (!hasSetParamValue && p.Direction == ParameterDirection.Input) {
            il.MarkLabel(lblParamIsInput);
            il.Emit(OpCodes.Ldloc_3); //hasSetParamValue
            il.Emit(OpCodes.Brtrue, lblHasSetParamValueCond);

            il.Emit(OpCodes.Ldloc_2); //p
            il.Emit(OpCodes.Callvirt, typeof(IDataParameter).GetMethod("get_Direction"));
            il.Emit(OpCodes.Ldc_I4_1); //ParameterDirection.Input
            il.Emit(OpCodes.Ceq);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Ceq);
            il.Emit(OpCodes.Br, lblHasSetParamValueCond2);

            il.MarkLabel(lblHasSetParamValueCond);
            il.Emit(OpCodes.Ldc_I4_1);
            il.MarkLabel(lblHasSetParamValueCond2);
            il.Emit(OpCodes.Stloc_1);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Brtrue, lblAddParamToCommand);

            il.Emit(OpCodes.Ldstr, "The required stored procedure parameter (");
            il.Emit(OpCodes.Ldloc_S, 5);
            il.Emit(OpCodes.Ldstr, ") does not exist in entity or was not specified!");
            il.Emit(OpCodes.Call, typeof(String).GetMethod("Concat", new Type[] { typeof(string), typeof(string), typeof(string) }));
            il.Emit(OpCodes.Ldloc_S, 5);
            il.Emit(OpCodes.Newobj, typeof(ArgumentException).GetConstructor(new Type[] { typeof(string), typeof(string) }));
            il.Emit(OpCodes.Throw);

            //}

            il.MarkLabel(lblAddParamToCommand);
            il.Emit(OpCodes.Ldarg_0); //command.Parameters.Add(p);
            il.Emit(OpCodes.Callvirt, typeof(IDbCommand).GetMethod("get_Parameters"));
            il.Emit(OpCodes.Ldloc_2); //instance int32 [mscorlib]System.Collections.IList::Add(object)
            il.Emit(OpCodes.Callvirt, typeof(IList).GetMethod("Add", new Type[] { typeof(object) }));
            il.Emit(OpCodes.Pop);


            //--------------------------------------------------------------------------------
            //Time to increament
            il.MarkLabel(lblIncreament);
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Add);
            il.Emit(OpCodes.Stloc_0);

            //index < paramList.Count
            il.MarkLabel(lblHasMoreParams);
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldarg_1); //[paramList]
            il.Emit(OpCodes.Callvirt, paramListType.GetMethod("get_Count"));
            //il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Clt); //If index < paramList.Count => push 1 to the eval stack else push 0
            il.Emit(OpCodes.Stloc_1);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Brtrue, lblLoadValueAgain);

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Stloc_S, 8);
            il.Emit(OpCodes.Br_S, lblReturn);

            il.MarkLabel(lblReturn);
            il.Emit(OpCodes.Ldloc_S, 8);
            il.Emit(OpCodes.Ret); //Return the command

            return (AttachParamToCommand)dm.CreateDelegate(typeof(AttachParamToCommand));

        }

        private static MethodInfo GetPropertySetter(PropertyInfo propertyInfo, Type type) {
            return propertyInfo.DeclaringType == type ?
                propertyInfo.GetSetMethod(true) :
                propertyInfo.DeclaringType.GetProperty(propertyInfo.Name,
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetSetMethod(true);
        }

        private static EntityDeserializer DeserializeEntity<TEntity>(IDataReader dr, int rowIndex, IDataParameterCollection parameters,
            IList<TEntity> entites, Type entityType, dynamic partialParam, bool implicitCast = true) where TEntity : class, new() {

            DynamicMethod dm = new DynamicMethod("EntityDeserializer" + Guid.NewGuid().ToString(),
                typeof(object), // => Return Type
                new Type[] { typeof(IDataReader), typeof(bool), typeof(object) }); //Parameters;

            ILGenerator il = dm.GetILGenerator();

            //Declare Variables
            //--------------------------------------------
            il.DeclareLocal(entityType); //0 => TEntity
            il.DeclareLocal(typeof(int)); //1 => length
            il.DeclareLocal(typeof(object[])); //2 => drvalues
            il.DeclareLocal(typeof(bool)); //3 => condition
            il.DeclareLocal(typeof(string)); //4 => columnName
            il.DeclareLocal(typeof(PropertyInfo)); //5 => prop
            il.DeclareLocal(typeof(MethodInfo)); //6 => methodinfo
            il.DeclareLocal(typeof(object)); //7 => currValue
            il.DeclareLocal(typeof(IDictionary<string, object>)); //8 => partialParamDic

            //--------------------------------------------

            Label lblReturn = il.DefineLabel();


            //New
            //-----------------------------------------------
            /*il.Emit(OpCodes.Newobj, typeof(ExpandoObject).GetConstructor(Type.EmptyTypes));
            il.Emit(OpCodes.Starg_S, 2);*/

            il.Emit(OpCodes.Ldarg_2);
            il.Emit(OpCodes.Isinst, typeof(IDictionary<string, object>));
            il.Emit(OpCodes.Stloc_S, 8);
            //-----------------------------------------------

            MethodInfo createInst = typeof(Activator).GetMethod("CreateInstance", Type.EmptyTypes);
            MethodInfo createInstOfTOutput = createInst.MakeGenericMethod(entityType);
            il.Emit(OpCodes.Call, createInstOfTOutput); //[TEntity]
            il.Emit(OpCodes.Stloc_0);

            il.Emit(OpCodes.Ldarg_0); //[dr]
            il.Emit(OpCodes.Callvirt, typeof(IDataRecord).GetMethod("get_FieldCount")); //[dr]
            il.Emit(OpCodes.Stloc_1);

            //object[] drvalues = new object[length];
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Newarr, typeof(Object));
            il.Emit(OpCodes.Stloc_2);

            for (int i = 0; i < dr.FieldCount; i++) {
                Label lblContinueLoop = il.DefineLabel();

                string columnName = dr.GetName(i);

                il.Emit(OpCodes.Ldstr, columnName);
                il.Emit(OpCodes.Stloc_S, 4);

                //if (implicitCast) {
                il.Emit(OpCodes.Ldarg_1); //=> implicitCast
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq);
                il.Emit(OpCodes.Stloc_3);
                il.Emit(OpCodes.Ldloc_3);
                il.Emit(OpCodes.Brtrue, lblContinueLoop);

                il.Emit(OpCodes.Ldarg_0); // stack is now [target][target][reader]
                EmitInt32(il, i); // stack is now [target][target][reader][index]
                il.Emit(OpCodes.Callvirt, getItem); // stack is now [target][target][value-as-object]
                il.Emit(OpCodes.Stloc_S, 7); // -> currValue

                //TODO: Needs to be optimized
                //prop.SetValue(entity, drvalues[i], null);
                PropertyInfo prop = entityType.GetProperty(columnName);
                if (prop != null) {
                    var nullUnderlyingType = Nullable.GetUnderlyingType(prop.PropertyType);
                    var unboxType = nullUnderlyingType != null && nullUnderlyingType.IsEnum ? nullUnderlyingType : prop.PropertyType;
                    MethodInfo methodInfo = GetPropertySetter(prop, typeof(TEntity)) ?? null;

                    if (methodInfo != null) {
                        il.BeginExceptionBlock();

                        il.Emit(OpCodes.Ldloc_0); //=> entity
                        il.Emit(OpCodes.Ldloc_S, 7); // -> currValue
                        il.Emit(OpCodes.Unbox_Any, unboxType);
                        //il.Emit(OpCodes.Call, typeof(Console).GetMethod("WriteLine", new Type[] { unboxType }));
                        //il.Emit(OpCodes.Unbox_Any, unboxType);
                        il.Emit(OpCodes.Callvirt, methodInfo); // stack is now [target] 

                        il.BeginCatchBlock(typeof(Exception));
                        il.EndExceptionBlock();
                    }
                } else {
                    il.Emit(OpCodes.Ldloc_S, 8); //8 => partialParamDic
                    il.Emit(OpCodes.Ldloc_S, 4); //4 => columnName
                    il.Emit(OpCodes.Ldloc_S, 7); //7 => currValue
                    il.Emit(OpCodes.Callvirt, typeof(IDictionary<string, object>).GetMethod("set_Item", new Type[] { typeof(string), typeof(object) }));


                }

                il.MarkLabel(lblContinueLoop);
            }

            il.MarkLabel(lblReturn);
            il.Emit(OpCodes.Ldloc_0);
            //il.Emit(OpCodes.Castclass, typeof(object));
            il.Emit(OpCodes.Ret);


            return (EntityDeserializer)dm.CreateDelegate(typeof(EntityDeserializer));


        }

        #endregion



        /// <summary>
        /// Executes a storedProcedure against the connString and returns the number of rows affected.
        /// It will also load the partialParam with the current row's non-TEntity values.
        /// 
        /// If you need the return value, access it dynamically from partialParam
        /// Note: @RETURN_VALUE is returned as ReturnValue
        /// </summary>
        /// <typeparam name="TEntity">The [TableName]VD associated with the table entity coming from the stored procedure.</typeparam>
        /// <param name="connString">The connection string that identifies the database server</param>
        /// <param name="storedProcedure">The stored procedure to execute</param>
        /// <param name="partialParam">Populated with row/record values that ExecuteReader can't find a corresponding property in your TEntity</param>
        /// <param name="entity">Populated with row/record values that ExecuteReader can't find a corresponding property in your TEntity</param>
        /// <returns>The number of rows affected</returns>
        public static int ExecuteNonQuery<TEntity>(IDbConnection connection, string storedProcedure, ref dynamic partialParam, TEntity entity = null) where TEntity : class, new() {
            int returnValue = -1;

            //db value don't exist in entity
            if (entity == null) entity = new TEntity();
            if (partialParam == null) partialParam = new ExpandoObject();
            if (partialParam.GetType() != typeof(ExpandoObject)) throw new Exception("Initialize the 'partialParam' parameter to an ExpandoObject!");

            var param = partialParam as IDictionary<string, object>;

            //TODO: partialParam take presedence over the entity properties. Basically to give you control
            //TODO: Output Parameters will return no value. Use ExecuteNonQuery or ExecuteScalar instead

            IList<TEntity> list = new List<TEntity>();
            using (connection) {
                using (IDbCommand command = connection.CreateCommand()) {
                    command.CommandText = storedProcedure;
                    command.CommandType = CommandType.StoredProcedure;

                    if (connection.State != ConnectionState.Open) connection.Open();

                    IList<IDataParameter> paramList = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedure, true);

                    //Initialize || Set param values
                    foreach (IDataParameter p in paramList) {
                        if (p.Direction != ParameterDirection.ReturnValue) {
                            bool hasSetParamValue = false;
                            string paramName = p.ParameterName.TrimStart('@');

                            //Check in the partial pram
                            //param = partialParam.Get(paramName);

                            if (HasPartialProperty(partialParam, paramName)) {
                                p.Value = param[paramName]; hasSetParamValue = true;
                            } else {
                                //Find the parameter in the entity 
                                PropertyInfo prop = typeof(TEntity).GetProperty(paramName);

                                if (prop != null) {
                                    p.Value = prop.GetValue(entity, null) ?? DBNull.Value; hasSetParamValue = true;
                                }
                            }

                            if (!hasSetParamValue && p.Direction == ParameterDirection.Input) {
                                throw new ArgumentException("The required stored procedure parameter (" + paramName + ") does not exist in entity or was not specified!", paramName);
                            }

                        }

                        command.Parameters.Add(p);
                    }

                    returnValue = command.ExecuteNonQuery();

                    foreach (SqlParameter p in command.Parameters) {
                        string _paramName = p.ParameterName.TrimStart('@');
                        if (_paramName == "RETURN_VALUE") {
                            param["ReturnValue"] = p.Value;
                        } else {
                            param[p.ParameterName.TrimStart('@')] = p.Value;
                        }
                    }

                    command.Parameters.Clear();
                }
            }

            return returnValue;
        }

        /// <summary>
        /// Executes the query, and returns the first column of the first row in the 
        /// resultset returned by the query. Extra columns or rows are ignored.
        /// It will also load the partialParam with the current row's non-TEntity values.
        /// 
        /// If you need the return value, access it dynamically from partialParam
        /// Note: @RETURN_VALUE is returned as ReturnValue
        /// </summary>
        /// <typeparam name="TEntity">The [TableName]VD associated with the table entity coming from the stored procedure.</typeparam>
        /// <typeparam name="TReturnValue">The type for the firs column of the first row in the resultset</typeparam>
        /// <param name="connString">The connection string that identifies the database server</param>
        /// <param name="storedProcedure">The stored procedure to execute</param>
        /// <param name="partialParam">Populated with row/record values that ExecuteReader can't find a corresponding property in your TEntity</param>
        /// <param name="entity">Populated with row/record values that ExecuteReader can't find a corresponding property in your TEntity</param>
        /// <returns>The first column of the first row in the resultset</returns>
        public static TReturnValue ExecuteScalar<TEntity, TReturnValue>(IDbConnection connection, string storedProcedure, ref dynamic partialParam, TEntity entity = null) where TEntity : class, new() {
            TReturnValue returnValue;

            //db value don't exist in entity
            if (entity == null) entity = new TEntity();
            if (partialParam == null) partialParam = new ExpandoObject();
            if (partialParam.GetType() != typeof(ExpandoObject)) throw new Exception("Initialize the 'partialParam' parameter to an ExpandoObject!");

            var param = partialParam as IDictionary<string, object>;

            //TODO: partialParam take presedence over the entity properties. Basically to give you control
            //TODO: Output Parameters will return no value. Use ExecuteNonQuery or ExecuteScalar instead

            IList<TEntity> list = new List<TEntity>();
            using (connection) {
                using (IDbCommand command = connection.CreateCommand()) {
                    command.CommandText = storedProcedure;
                    command.CommandType = CommandType.StoredProcedure;

                    if (connection.State != ConnectionState.Open) connection.Open();

                    IList<IDataParameter> paramList = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedure, true);

                    //Initialize || Set param values
                    foreach (IDataParameter p in paramList) {
                        if (p.Direction != ParameterDirection.ReturnValue) {
                            bool hasSetParamValue = false;
                            string paramName = p.ParameterName.TrimStart('@');

                            //Check in the partial pram
                            //param = partialParam.Get(paramName);

                            if (HasPartialProperty(partialParam, paramName)) {
                                p.Value = param[paramName]; hasSetParamValue = true;
                            } else {
                                //Find the parameter in the entity 
                                PropertyInfo prop = typeof(TEntity).GetProperty(paramName);

                                if (prop != null) {
                                    p.Value = prop.GetValue(entity, null) ?? DBNull.Value; hasSetParamValue = true;
                                }
                            }

                            if (!hasSetParamValue && p.Direction == ParameterDirection.Input) {
                                throw new ArgumentException("The required stored procedure parameter (" + paramName + ") does not exist in entity or was not specified!", paramName);
                            }

                        }

                        command.Parameters.Add(p);
                    }

                    var o = command.ExecuteScalar();
                    returnValue = (o == null) ? default(TReturnValue) : (TReturnValue)o;

                    foreach (SqlParameter p in command.Parameters) {
                        string _paramName = p.ParameterName.TrimStart('@');
                        if (_paramName == "RETURN_VALUE") {
                            param["ReturnValue"] = p.Value;
                        } else {
                            param[p.ParameterName.TrimStart('@')] = p.Value;
                        }
                    }

                    command.Parameters.Clear();
                }
            }

            return returnValue;
        }

        /// <summary>
        /// Executes the storedProcedure against the connString and returns a IEnumerable<TEntity>
        /// It will also load the partialParam with the current row's non-TEntity values.
        /// 
        /// If you need the return value, access it dynamically from partialParam
        /// Note: @RETURN_VALUE is returned as ReturnValue
        /// 
        /// NOTE: Shouldn't forget to initialize the partialParam as an ExpandoObject
        /// </summary>
        /// <typeparam name="TEntity">The [TableName]VD associated with the table entity coming from the stored procedure.</typeparam>
        /// <param name="connString">The connection string that identifies the database server</param>
        /// <param name="storedProcedure">The stored procedure to execute</param>
        /// <param name="partialParam">Populated with row/record values that ExecuteReader can't find a corresponding property in your TEntity</param>
        /// <param name="entity">An entity that contain values which should be used as parameters for storedProcedure</param>
        /// <param name="includeReturnValue">Specify if the Return Value from the storedProcedure should be included</param>
        /// <param name="implicitCast">Support of implicit or explict cast</param>
        /// <returns></returns>
        public static IEnumerable<TEntity> ExecuteReader<TEntity>(IDbConnection connection, string storedProcedure, dynamic partialParam, TEntity entity = null, bool includeReturnValue = true, bool implicitCast = true) where TEntity : class, new() {
            
            //db value don't exist in entity
            if (partialParam == null) partialParam = new ExpandoObject();
            if (entity == null) entity = new TEntity();

            //if (partialParam == null) throw new Exception("Initialize the 'partialParam' parameter to an ExpandoObject!");
            if (partialParam.GetType() != typeof(ExpandoObject)) throw new Exception("Initialize the 'partialParam' parameter to an ExpandoObject!");

            var param = partialParam as IDictionary<string, object>;

            //NOTE: partialParam take presedence over the entity properties. Basically to give you control
            //NOTE: Output Parameters will return no value. Use ExecuteNonQuery or ExecuteScalar instead
            Type entityType = typeof(TEntity);
            IList<TEntity> entites = new List<TEntity>();

            using (connection) {
                using (IDbCommand command = connection.CreateCommand()) {
                    command.CommandText = storedProcedure;
                    command.CommandType = CommandType.StoredProcedure;

                    if (connection.State != ConnectionState.Open) connection.Open();//new List<IDataParameter>(); // 

                    List<IDataParameter> paramList = (List<IDataParameter>)SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedure, includeReturnValue);


                    /*
                    For now, use the foreach 
                    string _key = storedProcedure + entityType.Name + paramList.GetHashCode().ToString();
                    AttachParamToCommand _genMethod;
                    TryGetQueryCache(_key, out _genMethod);
                    if (_genMethod == null) {
                        _genMethod = GenerateAttachParamToCommand<TEntity>(command, paramList, ref partialParam);
                        SetQueryCache(_key, _genMethod);
                    }
                    _genMethod(command, paramList, entity, ref partialParam);*/


                    //Initialize || Set param values
                    foreach (IDataParameter p in paramList) {
                        if (p.Direction != ParameterDirection.ReturnValue) {
                            bool hasSetParamValue = false;
                            string paramName = p.ParameterName.TrimStart('@');

                            if (HasPartialProperty(partialParam, paramName)) {
                                p.Value = param[paramName]; hasSetParamValue = true;
                            } else {
                                //Find the parameter in the entity 
                                PropertyInfo prop = typeof(TEntity).GetProperty(paramName);

                                if (prop != null) {
                                    p.Value = prop.GetValue(entity, null) ?? DBNull.Value; hasSetParamValue = true;
                                }
                            }

                            if (!hasSetParamValue && p.Direction == ParameterDirection.Input) {
                                throw new ArgumentException("The required stored procedure parameter (" + paramName + ") does not exist in entity or was not specified!", paramName);
                            }

                        }

                        command.Parameters.Add(p);
                    }


                    using (IDataReader dr = command.ExecuteReader()) {
                        if (typeof(TEntity).IsClass) {
                            int rowIndex = 0;
                            string _drClassDeserializerKey = storedProcedure + entityType.Name;
                            EntityDeserializer _drClassDeserializer;
                            TryGetQueryCache(_drClassDeserializerKey, out _drClassDeserializer);
                            if (_drClassDeserializer == null) {
                                _drClassDeserializer = DeserializeEntity<TEntity>(dr,
                                ++rowIndex, command.Parameters, entites, entityType, partialParam, implicitCast);

                                SetQueryCache(_drClassDeserializerKey, _drClassDeserializer);
                            }

                            //TODO: Get table columns and cache it
                            while (dr.Read()) { //true => implicitCast
                                /*var entityToReturn = _drClassDeserializer(dr, true, partialParam);
                                if (entityToReturn != null) {
                                    yield return (TEntity)entityToReturn;
                                } else {
                                    yield break;
                                }*/

                                yield return (TEntity)_drClassDeserializer(dr, true, partialParam);
                            }
                        } else {
                            throw new Exception("Non-Class entity types are not supported for ExecuteReader");
                        }
                    }
                }
            }

            yield break;
        }

        /// <summary>
        /// Executes the storedProcedure against the connString and returns a IEnumerable<TEntity>
        /// It will also load the partialParam with the current row's non-TEntity values.
        /// 
        /// If you need the return value, access it dynamically from partialParam
        /// Note: @RETURN_VALUE is returned as ReturnValue
        /// 
        /// NOTE: Shouldn't forget to initialize the partialParam as an ExpandoObject
        /// </summary>
        /// <typeparam name="TEntity">The [TableName]VD associated with the table entity coming from the stored procedure.</typeparam>
        /// <param name="connString">The connection string that identifies the database server</param>
        /// <param name="storedProcedure">The stored procedure to execute</param>
        /// <param name="partialParam">Populated with row/record values that ExecuteReader can't find a corresponding property in your TEntity</param>
        /// <param name="entity">An entity that contain values which should be used as parameters for storedProcedure</param>
        /// <param name="includeReturnValue">Specify if the Return Value from the storedProcedure should be included</param>
        /// <param name="implicitCast">Support of implicit or explict cast</param>
        /// <returns></returns>
        public static TEntity ExecuteReaderSingle<TEntity>(IDbConnection connection, string storedProcedure, dynamic partialParam, TEntity entity = null, bool includeReturnValue = true, bool implicitCast = true) where TEntity : class, new() {

            //db value don't exist in entity
            if (partialParam == null) partialParam = new ExpandoObject();
            if (entity == null) entity = new TEntity();

            //if (partialParam == null) throw new Exception("Initialize the 'partialParam' parameter to an ExpandoObject!");
            if (partialParam.GetType() != typeof(ExpandoObject)) throw new Exception("Initialize the 'partialParam' parameter to an ExpandoObject!");

            var param = partialParam as IDictionary<string, object>;

            //NOTE: partialParam take presedence over the entity properties. Basically to give you control
            //NOTE: Output Parameters will return no value. Use ExecuteNonQuery or ExecuteScalar instead
            Type entityType = typeof(TEntity);
            IList<TEntity> entites = new List<TEntity>();

            using (connection) {
                using (IDbCommand command = connection.CreateCommand()) {
                    command.CommandText = storedProcedure;
                    command.CommandType = CommandType.StoredProcedure;

                    if (connection.State != ConnectionState.Open) connection.Open();//new List<IDataParameter>(); // 

                    List<IDataParameter> paramList = (List<IDataParameter>)SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedure, includeReturnValue);


                    /*
                    For now, use the foreach 
                    string _key = storedProcedure + entityType.Name + paramList.GetHashCode().ToString();
                    AttachParamToCommand _genMethod;
                    TryGetQueryCache(_key, out _genMethod);
                    if (_genMethod == null) {
                        _genMethod = GenerateAttachParamToCommand<TEntity>(command, paramList, ref partialParam);
                        SetQueryCache(_key, _genMethod);
                    }
                    _genMethod(command, paramList, entity, ref partialParam);*/


                    //Initialize || Set param values
                    foreach (IDataParameter p in paramList) {
                        if (p.Direction != ParameterDirection.ReturnValue) {
                            bool hasSetParamValue = false;
                            string paramName = p.ParameterName.TrimStart('@');

                            if (HasPartialProperty(partialParam, paramName)) {
                                p.Value = param[paramName]; hasSetParamValue = true;
                            } else {
                                //Find the parameter in the entity 
                                PropertyInfo prop = typeof(TEntity).GetProperty(paramName);

                                if (prop != null) {
                                    p.Value = prop.GetValue(entity, null) ?? DBNull.Value; hasSetParamValue = true;
                                }
                            }

                            if (!hasSetParamValue && p.Direction == ParameterDirection.Input) {
                                throw new ArgumentException("The required stored procedure parameter (" + paramName + ") does not exist in entity or was not specified!", paramName);
                            }

                        }

                        command.Parameters.Add(p);
                    }


                    using (IDataReader dr = command.ExecuteReader()) {
                        if (typeof(TEntity).IsClass) {
                            int rowIndex = 0;
                            string _drClassDeserializerKey = storedProcedure + entityType.Name;
                            EntityDeserializer _drClassDeserializer;
                            TryGetQueryCache(_drClassDeserializerKey, out _drClassDeserializer);
                            if (_drClassDeserializer == null) {
                                _drClassDeserializer = DeserializeEntity<TEntity>(dr,
                                ++rowIndex, command.Parameters, entites, entityType, partialParam, implicitCast);

                                SetQueryCache(_drClassDeserializerKey, _drClassDeserializer);
                            }

                            //TODO: Get table columns and cache it
                            if (dr.Read()) { //true => implicitCast
                                return (TEntity)_drClassDeserializer(dr, true, partialParam) ?? new TEntity();
                            }
                        } else {
                            throw new Exception("Non-Class entity types are not supported for ExecuteReader");
                        }
                    }
                }
            }

            return new TEntity();
        }

    }
}
