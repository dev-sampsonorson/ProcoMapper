using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections.Concurrent;

namespace Blendy.Core.DALayer.EntityMapper {

    public interface IPartialParamInfo {
        string Name { get; }
        Type TypeOfValue { get; }
        ParameterDirection Direction { get; }

        object GetValue();
    }
    public class PartialParamInfo<TValue> : IPartialParamInfo {
        public string Name { get; private set; }
        public TValue Value { get; private set; }
        public Type TypeOfValue { get; private set; }
        public ParameterDirection Direction { get; private set; }
        public object GetValue() {
            return (Value != null)? Value : default(TValue);
        }

        public PartialParamInfo(Type typeOfValue, string name, TValue value, ParameterDirection direction) {
            TypeOfValue = typeOfValue;
            Name = name;
            Value = value;
            Direction = direction;
        }
    }

    public interface IPartialParam {
        void Add(Type tValue, string name, object value, ParameterDirection direction);
        void Add<TValue>(string name, TValue value, ParameterDirection direction);
        //PartialParamInfo<object> Get(string name);
        //PartialParamInfo<TValue> Get<TValue>(string name);
        IPartialParamInfo Get(string name);
        object GetValue(string name);
        int GetReturnValue();
        TValue GetValue<TValue>(string name, bool implicitCastOnly = true);

        IEnumerable<IPartialParamInfo> GetParameters();
        //IEnumerable<PartialParamInfo<object>> GetParameters();
        //IEnumerable<PartialParamInfo<TValue>> GetParameters<TValue>();

    }

    public class PartialParam : IPartialParam {
        
        //public int? Size { get; set; }
        //public DbType? DbType { get; set; }
        //public IDbDataParameter AttachedParam { get; set; }

        private ConcurrentDictionary<string, IPartialParamInfo> parameters = new ConcurrentDictionary<string, IPartialParamInfo>();

        public void Add(Type tValue, string name, object value, ParameterDirection direction) {
            parameters.AddOrUpdate(name, new PartialParamInfo<object>(tValue, name, value, direction), (key, paramInfo) => {
                PartialParamInfo<object> existing = (PartialParamInfo<object>)paramInfo;

                if (!existing.Value.Equals(value) || existing.TypeOfValue != tValue || existing.Direction != direction) {
                    return new PartialParamInfo<object>(tValue, name, value, direction);
                }

                return existing;
            });
        }
        public void Add<TValue>(string name, TValue value, ParameterDirection direction) {
            parameters.AddOrUpdate(name, new PartialParamInfo<TValue>(typeof(TValue), name, value, direction), (key, paramInfo) => {
                PartialParamInfo<TValue> existing = (PartialParamInfo<TValue>)paramInfo;

                if (!existing.Value.Equals(value) || existing.TypeOfValue != typeof(TValue) || existing.Direction != direction) {
                    return new PartialParamInfo<TValue>(typeof(TValue), name, value, direction);
                }

                return existing;
            });
        }

        public IPartialParamInfo Get(string name) {
            IPartialParamInfo parameter;
            parameters.TryGetValue(name, out parameter);
            return parameter;
        }
        public object GetValue(string name) {
            IPartialParamInfo parameter;
            parameters.TryGetValue(name, out parameter);
            return parameter.GetValue();
        }
        public int GetReturnValue() {
            IPartialParamInfo parameter;
            parameters.TryGetValue("RETURN_VALUE", out parameter);
            return (parameter != null)? (int)parameter.GetValue() : default(int);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="name"></param>
        /// <returns></returns>
        public TValue GetValue<TValue>(string name, bool implicitCastOnly = true) {
            IPartialParamInfo parameter;
            parameters.TryGetValue(name, out parameter);
            object value = (parameter != null) ? parameter.GetValue() : null;

            if (value == null) return default(TValue);

            if (implicitCastOnly) return (TValue)value;

            if (value.GetType().IsCastableTo(typeof(TValue))) {
                return (TValue)value.CastTo(typeof(TValue));
            }

            throw new InvalidCastException("The value can't be cast from " + value.GetType().Name + " to " + typeof(TValue).Name);
        }

        public IEnumerable<IPartialParamInfo> GetParameters() {
            foreach (KeyValuePair<string, IPartialParamInfo> pair in parameters)
                yield return pair.Value;
        }


        /*
         * 
         * 
         * 
         * 
        public PartialParamInfo<object> Get(string name) {
            IPartialParamInfo parameter;
            parameters.TryGetValue(name, out parameter);
            return (PartialParamInfo<object>)parameter;
        }
        public PartialParamInfo<TValue> Get<TValue>(string name) {
            IPartialParamInfo parameter;
            parameters.TryGetValue(name, out parameter);
            return (PartialParamInfo<TValue>)parameter;
        }
        public IEnumerable<PartialParamInfo<object>> GetParameters() {
            foreach (KeyValuePair<string, IPartialParamInfo> pair in parameters)
                yield return (PartialParamInfo<object>)pair.Value;
        }
        public IEnumerable<PartialParamInfo<TValue>> GetParameters<TValue>() {
            foreach (KeyValuePair<string, IPartialParamInfo> pair in parameters)
                yield return (PartialParamInfo<TValue>)pair.Value;
        }

        */
    }
}
