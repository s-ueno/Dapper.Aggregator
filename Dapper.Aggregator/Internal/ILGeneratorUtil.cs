using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{



    internal static class ILGeneratorUtil
    {
        private static readonly ConcurrentDictionary<Type, Type> TypeCache = new ConcurrentDictionary<Type, Type>();
        //static ILGeneratorUtil()
        //{
        //    var assemblyName = new AssemblyName(Guid.NewGuid().ToString());
        //    dynamicAssemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
        //}
        //private static readonly AssemblyBuilder dynamicAssemblyBuilder;

        public static bool IsInjected(Type type)
        {
            ValidColumnTypeMap(type);
            return TypeCache.ContainsKey(type);
        }

        public static Type InjectionInterfaceWithProperty(Type targetType)
        {
            Type buildType;
            if (TypeCache.TryGetValue(targetType, out buildType))
            {
                return buildType;
            }

            var assemblyName = new AssemblyName(Guid.NewGuid().ToString());
            var dynamicAssemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            var moduleBuilder = dynamicAssemblyBuilder.DefineDynamicModule("ILGeneratorUtil." + targetType.Name);
            var typeBuilder = moduleBuilder.DefineType(targetType.Name + "_" + Guid.NewGuid(), TypeAttributes.Public | TypeAttributes.Class, targetType);

            var interfaceType = typeof(IContainerHolder);

            typeBuilder.AddInterfaceImplementation(interfaceType);
            foreach (var each in interfaceType.GetProperties())
            {
                BuildProperty(typeBuilder, each.Name, each.PropertyType);
            }
            buildType = typeBuilder.CreateTypeInfo();
            TypeCache[targetType] = buildType;

            ValidColumnTypeMap(targetType);
            ValidColumnTypeMap(buildType);
            return buildType;
        }
        private static void BuildProperty(TypeBuilder typeBuilder, string name, Type type)
        {
            var field = typeBuilder.DefineField("_" + name.ToLower(), type, FieldAttributes.Private);
            var propertyBuilder = typeBuilder.DefineProperty(name, System.Reflection.PropertyAttributes.None, type, null);

            var getSetAttr = MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName | MethodAttributes.Virtual;

            var getter = typeBuilder.DefineMethod("get_" + name, getSetAttr, type, Type.EmptyTypes);
            var getIL = getter.GetILGenerator();
            getIL.Emit(OpCodes.Ldarg_0);
            getIL.Emit(OpCodes.Ldfld, field);
            getIL.Emit(OpCodes.Ret);

            var setter = typeBuilder.DefineMethod("set_" + name, getSetAttr, null, new Type[] { type });
            var setIL = setter.GetILGenerator();
            setIL.Emit(OpCodes.Ldarg_0);
            setIL.Emit(OpCodes.Ldarg_1);
            setIL.Emit(OpCodes.Stfld, field);
            setIL.Emit(OpCodes.Ret);

            propertyBuilder.SetGetMethod(getter);
            propertyBuilder.SetSetMethod(setter);
        }

        private static void ValidColumnTypeMap(Type t)
        {
            if (!ColumnAttMapped.Contains(t))
            {
                Dapper.SqlMapper.SetTypeMap(t, new ColumnAttributeTypeMapper(t));
                ColumnAttMapped.Add(t);
            }
        }

        //http://stackoverflow.com/questions/8902674/manually-map-column-names-with-class-properties
        private static readonly ConcurrentBag<Type> ColumnAttMapped = new ConcurrentBag<Type>();
        private class ColumnAttributeTypeMapper : MultiTypeMapper
        {
            public ColumnAttributeTypeMapper(Type t)
                : base(new SqlMapper.ITypeMap[]
                {
                    new CustomPropertyTypeMap(t, FindPropertyInfo),
                    new DefaultTypeMap(t)
                })
            {
            }
            private static PropertyInfo FindPropertyInfo(Type t, string columnName)
            {
                try
                {
                    var properties = t.GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).ToArray();
                    foreach (var each in properties)
                    {
                        var atts = each.GetCustomAttributes<ColumnAttribute>(true).ToArray();
                        var columnAtt = atts.SingleOrDefault();
                        if (columnAtt != null && string.Compare(columnAtt.Name, columnName, true) == 0) return each;
                        var columnInfo = atts.OfType<ColumnAttribute>().FirstOrDefault();
                        if (columnInfo != null && string.Compare(columnInfo.Name, columnName, true) == 0) return each;

                        //エスケープ対応
                        if (columnInfo != null && string.Compare(columnInfo.Name, string.Format("\"{0}\"", columnName), true) == 0) return each;
                        if (columnInfo != null && string.Compare(columnInfo.Name, string.Format("'{0}'", columnName), true) == 0) return each;
                    }
                }
                catch
                {
                }
                return null;
            }

        }
        private class MultiTypeMapper : SqlMapper.ITypeMap
        {
            private readonly IEnumerable<SqlMapper.ITypeMap> _mappers;
            public MultiTypeMapper(IEnumerable<SqlMapper.ITypeMap> mappers) { _mappers = mappers; }
            public ConstructorInfo FindConstructor(string[] names, Type[] types)
            {
                foreach (var mapper in _mappers)
                {
                    try
                    {
                        var result = mapper.FindConstructor(names, types);
                        if (result != null) return result;
                    }
                    catch (NotImplementedException) { }
                }
                return null;
            }
            public SqlMapper.IMemberMap GetConstructorParameter(ConstructorInfo constructor, string columnName)
            {
                foreach (var mapper in _mappers)
                {
                    try
                    {
                        var result = mapper.GetConstructorParameter(constructor, columnName);
                        if (result != null) return result;
                    }
                    catch (NotImplementedException) { }
                }
                return null;
            }
            public SqlMapper.IMemberMap GetMember(string columnName)
            {
                foreach (var mapper in _mappers)
                {
                    try
                    {
                        var result = mapper.GetMember(columnName);
                        if (result != null) return result;
                    }
                    catch (NotImplementedException) { }
                }
                return null;
            }
            public ConstructorInfo FindExplicitConstructor()
            {
                return _mappers.Select(mapper => mapper.FindExplicitConstructor()).FirstOrDefault(result => result != null);
            }
        }
    }
}
