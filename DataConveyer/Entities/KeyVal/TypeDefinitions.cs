//TypeDefinitions.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using System;
using System.Collections.Concurrent;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Set of memoized functions that take one parameter: Key (i.e. field name) and return attribute specific to the field type
   /// </summary>
   internal class TypeDefinitions
   {
      /// <summary>
      /// Intended as a function to return a type for a given field (e.g. ItemType.Int)
      /// </summary>
      internal readonly Func<string, ItemType> GetFldType;

      /// <summary>
      /// Intended as a function to return an output format for a given field (e.g. "##,000.00")
      /// </summary>
      internal readonly Func<string, string> GetFldFormat;

      /// <summary>
      /// Intended as a function to return a parsing function for a given type (val -> typedVal function, e.g. s => Int.Parse(s)... but it should return default value if parse fails)
      /// </summary>
      internal readonly Func<string, Func<string, object>> GetFldParser;

      /// <summary>
      /// Intended as a function to return an item creating function for a given type (val -> kvItem function, where val is string)
      /// </summary>
      internal readonly Func<string, Func<string, IItem>> GetFldCreator;

      /// <summary>
      /// Intended as a function to return an item creating function for a given type (val -> kvItem function, where val is a specific type, e.g. int or Decimal)
      /// </summary>
      internal readonly Func<string, Func<object, IItem>> GetFldCreatorEx;


      /// <summary>
      /// Create an instance based on functions + initial values for field types and formats
      /// </summary>
      /// <param name="fldTypeFunc"></param>
      /// <param name="initFldTypes"></param>
      /// <param name="fldFormatFunc"></param>
      /// <param name="initFldFormats"></param>
      internal TypeDefinitions(Func<string, ItemType> fldTypeFunc,
                         ConcurrentDictionary<string, ItemType> initFldTypes,
                         Func<string, string> fldFormatFunc,
                         ConcurrentDictionary<string, string> initFldFormats)
      {
         GetFldType = Memoize<string, ItemType>(fldTypeFunc, initFldTypes);
         GetFldFormat = Memoize<string, string>(fldFormatFunc, initFldFormats);

         //Field parsing function is a straight derivation from the field type (also, nothing to cache up-front)
         GetFldParser = Memoize<string, Func<string, object>>(k => FldParserForAType(k), null);

         //Field creating function is a straight derivation from the field type (empty cache up-front)
         GetFldCreator = Memoize<string, Func<string, IItem>>(k => FldCreatorForAType(k), null);

         //Field creating function is a straight derivation from the field type (empty cache up-front)
         GetFldCreatorEx = Memoize<string, Func<object, IItem>>(k => FldCreatorForATypeEx(k), null);

      }  //ctor

      //TODO: Consider merging FldParserForAType and FldCreatorForAType into a single function (tricky!)

      /// <summary>
      /// Helper function that returns a function to be returned by GetFldParser
      /// </summary>
      /// <param name="key"></param>
      /// <returns></returns>
      private Func<string, object> FldParserForAType(string key)
      {
         return val =>
         {
            switch (GetFldType(key))
            {
               case ItemType.Bool:
                  return ParseTo<bool>(val, bool.TryParse);

               case ItemType.Int:
                  return ParseTo<int>(val, int.TryParse);

               case ItemType.DateTime:
                  return ParseTo<DateTime>(val, DateTime.TryParse);

               case ItemType.Decimal:
                  return ParseTo<Decimal>(val, Decimal.TryParse);

               case ItemType.String:
                  return val;

               default: //Void
                  return null;  //"impossible" condition as Void type should never be defined for a key
            }
         };
      }

      /// <summary>
      /// Helper function that returns a function to be returned by GetFldCreator
      /// </summary>
      /// <param name="key"></param>
      /// <returns></returns>
      private Func<string, IItem> FldCreatorForAType(string key)
      {
         return val =>
         {
            var def = new ItemDef(GetFldType(key), GetFldFormat(key));

            switch (GetFldType(key))
            {
               case ItemType.Bool:
                  return CreateItemOf<bool>(key, val, def, bool.TryParse);

               case ItemType.Int:
                  return CreateItemOf<int>(key, val, def, int.TryParse);

               case ItemType.DateTime:
                  return CreateItemOf<DateTime>(key, val, def, DateTime.TryParse);

               case ItemType.Decimal:
                  return CreateItemOf<Decimal>(key, val, def, Decimal.TryParse);

               case ItemType.String:
                  if (val == null) return new KeyValItem<string>(key, null, def);
                  return new KeyValItem<string>(key, val, def);

               default: //Void
                  throw new InvalidOperationException($"Void KeyValItem cannot be created for a key of '{key}'");  //"impossible" condition as Void item should never be defined in type defs
            }
         };
      }
      /// <summary>
      /// Helper function that returns a function to be returned by GetFldCreatorEx
      /// </summary>
      /// <param name="key"></param>
      /// <returns></returns>
      private Func<object, IItem> FldCreatorForATypeEx(string key)
      {
         return val =>
         {
            var def = new ItemDef(GetFldType(key), GetFldFormat(key));

            switch (GetFldType(key))
            {
               case ItemType.Bool:
                  //if (val is bool) return new KeyValItem<bool>(key, (bool)val, def);  //<- not really needed as CreateItemOf<> w/2nd parm object will do the same
                  return CreateItemOf<bool>(key, val, def, bool.TryParse);

               case ItemType.Int:
                  //if (val is int) return new KeyValItem<int>(key, (int)val, def);  //<- not really needed as CreateItemOf<> w/2nd parm object will do the same
                  return CreateItemOf<int>(key, val, def, int.TryParse);

               case ItemType.DateTime:
                  return CreateItemOf<DateTime>(key, val, def, DateTime.TryParse);

               case ItemType.Decimal:
                  return CreateItemOf<Decimal>(key, val, def, Decimal.TryParse);

               case ItemType.String:
                  if (val == null) return new KeyValItem<string>(key, null, def);
                  return new KeyValItem<string>(key, val.ToString(), def);

               default: //Void
                  throw new InvalidOperationException($"Void KeyValItem cannot be created for a key of '{key}'");  //"impossible" condition as Void item should never be defined in type defs
            }
         };
      }


      /// <summary>
      /// Helper function to parse an object into an object of a given value type
      /// </summary>
      /// <typeparam name="T">One of the "parse-able" value types, like int, DateTime or Decimal</typeparam>
      /// <param name="value">Value to parse (can be of an type that supports ToString method)</param>
      /// <param name="tryParse">TryParse method for the type T</param>
      /// <returns>Parsed value of type T or default(T) if parse failed</returns>
      private static T ParseTo<T>(object value, TryParse<T> tryParse)
      {
         //inspired by http://stackoverflow.com/questions/2961656/generic-tryparse
         if (value is T) return (T)value;
         if (value == null) return default;
         if (tryParse(value.ToString(), out T typedVal))
         {
            return typedVal;
         }
         //TODO: Consider throwing exception (?) - to be caught for error reporting
         return default;
      }
      /// <summary>
      /// Helper function to parse a string into an object of a given type (that supports TryParse method)
      /// </summary>
      private static T ParseTo<T>(string value, TryParse<T> tryParse)
      {
         if (tryParse(value.ToString(), out T typedVal))
         {
            return typedVal;
         }
         //TODO: Consider throwing exception (?) - to be caught for error reporting
         return default;
      }
      /// <summary>
      /// Delegate matching TryParse method for types, such as int, DateTime or Decimal
      /// </summary>
      /// <typeparam name="T">Value type</typeparam>
      /// <param name="s">string to parse</param>
      /// <param name="result">result</param>
      /// <returns></returns>
      private delegate bool TryParse<T>(string s, out T result);


      /// <summary>
      /// Helper function to create a strong typed KeyValItem of a given value type
      /// </summary>
      /// <typeparam name="T">One of the "parse-able" value types, like int, DateTime or Decimal</typeparam>
      /// <param name="key">Key of the item to create</param>
      /// <param name="value">Value of the item to create</param>
      /// <param name="def">Definition of the item to create; it's Type must match T (!)</param>
      /// <param name="tryParse">TryParse method for the type T</param>
      /// <returns></returns>
      private static KeyValItem<T> CreateItemOf<T>(string key, object value, ItemDef def, TryParse<T> tryParse)
      {
         if (value is T) return new KeyValItem<T>(key, (T)value, def);
         if (value == null) return new KeyValItem<T>(key, default, def);
         //here, val is of different type than needed; let's convert it
         if (tryParse(value.ToString(), out T typedVal))
         {
            return new KeyValItem<T>(key, typedVal, def);
         }
         //TODO: Report error that value parsing failed and default was used (MessageCreate event?)
         return new KeyValItem<T>(key, default, def);
      }
      private static KeyValItem<T> CreateItemOf<T>(string key, string value, ItemDef def, TryParse<T> tryParse)
      {
         if (tryParse(value, out T typedVal))
         {
            return new KeyValItem<T>(key, typedVal, def);
         }
         //TODO: Report error that value parsing failed and default was used (MessageCreate event?)
         return new KeyValItem<T>(key, default, def);
      }

      /// <summary>
      /// Memoizer of a pure, single argument function (conceivably expensive and/or called multiple times with same argument)
      /// It remembers result from prior call and uses it instead of calling the function again.
      /// </summary>
      /// <typeparam name="TIn">Type of the function input</typeparam>
      /// <typeparam name="TOut">Type of the function ouput</typeparam>
      /// <param name="func">Function that takes TIn and returns TOut</param>
      /// <param name="cache">Set of argument/result pairs to be memoized up front (they do not have to match what func would've calculated</param>
      /// <returns></returns>
      private Func<TIn, TOut> Memoize<TIn, TOut>(Func<TIn, TOut> func, ConcurrentDictionary<TIn, TOut> cache)
      {
         // Based on Joe Albahari's "Programming with Purity" (https://www.youtube.com/watch?v=aZCzG2I8Hds, 
         // https://www.linqpad.net/RichClient/ProgrammingWithPurity.zip), except that ConcurrentDictionary (and not Dictionary) is used.
         // (note that a NullReferenceException was occasionally thrown by Dictionary internal code when cache[input] = func(input)
         //  was executed; "Stack:   at System.Collections.Generic.Dictionary`2.Insert(TKey key, TValue value, Boolean add)
         //                          at System.Collections.Generic.Dictionary`2.set_Item(TKey key, TValue value)"
         if (cache == null) cache = new ConcurrentDictionary<TIn, TOut>();
         return (input =>
         {
            if (cache.TryGetValue(input, out TOut result)) return result;
            return cache.GetOrAdd(input, func(input));
         });
      }
   }
}
