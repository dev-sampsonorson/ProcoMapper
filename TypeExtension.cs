using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Reflection;
using System.Linq.Expressions;
using System.ComponentModel;

namespace Blendy.Core.DALayer.EntityMapper {
    public static class TypeExtension {




        static ConcurrentDictionary<Type, List<Type>> implicitDict = new ConcurrentDictionary<Type, List<Type>>();
        static ConcurrentDictionary<Type, List<Type>> explicitDict = new ConcurrentDictionary<Type, List<Type>>();


        public static void InitDictionary() {

            if (implicitDict.Count <= 0) {
                implicitDict[typeof(byte)] = new List<Type> { typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(sbyte)] = new List<Type> { typeof(short), typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(int)] = new List<Type> { typeof(long), typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(uint)] = new List<Type> { typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(short)] = new List<Type> { typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(ushort)] = new List<Type> { typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(long)] = new List<Type> { typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(ulong)] = new List<Type> { typeof(float), typeof(double), typeof(decimal) };
                implicitDict[typeof(float)] = new List<Type> { typeof(double) };
                implicitDict[typeof(char)] = new List<Type> { typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal) };
            }

            if (explicitDict.Count <= 0) {
                explicitDict[typeof(byte)] = new List<Type> { typeof(sbyte), typeof(char) };
                explicitDict[typeof(sbyte)] = new List<Type> { typeof(byte), typeof(ushort), typeof(uint), typeof(ulong), typeof(char) };
                explicitDict[typeof(int)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(uint), typeof(ulong), typeof(char) };
                explicitDict[typeof(uint)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(char) };
                explicitDict[typeof(short)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(ushort), typeof(uint), typeof(ulong), typeof(char) };
                explicitDict[typeof(ushort)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(char) };
                explicitDict[typeof(long)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(ulong), typeof(char) };
                explicitDict[typeof(ulong)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(char) };
                explicitDict[typeof(float)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(char), typeof(decimal) };
                explicitDict[typeof(double)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(char), typeof(float), typeof(decimal) };
                explicitDict[typeof(char)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short) };
                explicitDict[typeof(decimal)] = new List<Type> { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(char), typeof(float), typeof(double) };
            }
        }

        public static bool IsImplicitlyCastableTo(this Type from, Type to) {
            InitDictionary();

            if (from.IsNullable() && !to.IsNullable()) return false;
            if (from.IsNullable() && to.IsNullable() && (implicitDict.ContainsKey(Nullable.GetUnderlyingType(from)) && implicitDict[Nullable.GetUnderlyingType(from)].Contains(Nullable.GetUnderlyingType(to)))) return true;
            if (!from.IsNullable() && to.IsNullable() && (implicitDict.ContainsKey(Nullable.GetUnderlyingType(from)) && implicitDict[Nullable.GetUnderlyingType(from)].Contains(Nullable.GetUnderlyingType(to)))) return true;


            return implicitDict.ContainsKey(from) && implicitDict[from].Contains(to);
        }

        public static bool IsExplicitlyCastableTo(this Type from, Type to) {
            InitDictionary();

            if (from.IsNullable() && !to.IsNullable()) return false;
            if (from.IsNullable() && to.IsNullable() && (explicitDict.ContainsKey(Nullable.GetUnderlyingType(from)) && explicitDict[Nullable.GetUnderlyingType(from)].Contains(Nullable.GetUnderlyingType(to)))) return true;
            if (!from.IsNullable() && to.IsNullable() && (explicitDict.ContainsKey(Nullable.GetUnderlyingType(from)) && explicitDict[Nullable.GetUnderlyingType(from)].Contains(Nullable.GetUnderlyingType(to)))) return true;
            
            return explicitDict.ContainsKey(from) && explicitDict[from].Contains(to);
        }

        public static bool IsCastableToUsingExp(this Type from, Type to) {
            Func<Expression, UnaryExpression> bodyFunction = body => Expression.Convert(body, to);
            ParameterExpression inp = Expression.Parameter(from, "inp");
            try {
                // If this succeeds then we can cast 'from' type to 'to' type using implicit coercion
                Expression.Lambda(bodyFunction(inp), inp).Compile();
                return true;
            } catch (InvalidOperationException) {
                return false;
            }
        }

        public static bool IsCastableTo(this Type from, Type to) {
            InitDictionary();

            if (to.IsAssignableFrom(from)) return true;

            if (from.IsImplicitlyCastableTo(to)) return true;

            if (from.IsExplicitlyCastableTo(to)) return true;

            if (from.IsCastableToUsingExp(to)) return true;

            bool castable = from.GetMethods(BindingFlags.Public | BindingFlags.Static)
                            .Any(
                                m => m.ReturnType == to &&
                                m.Name == "op_Implicit" ||
                                m.Name == "op_Explicit"
                            );            

            return castable;
        }


        public static bool CanConvertTo(this string value, Type type) {
            TypeConverter converter = TypeDescriptor.GetConverter(type);
            return converter.IsValid(value);
        }
        

        public static bool IsNullable(this Type type) {
            return type.IsGenericType
            && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>));
        }
    }
}
