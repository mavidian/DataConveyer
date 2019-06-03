//KeyValItem.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


//Contents:
//  - class KeyValItem<TVal>
//  - static class KeyValItem (factory)

using System;
using System.Diagnostics;

namespace Mavidian.DataConveyer.Entities.KeyVal
{

   /// <summary>
   /// Represents a single pair of key and value. Objects of this class are immutable.
   /// </summary>
   internal class KeyValItem<TVal> : IItem, ITypedItem<TVal>
   {
      private readonly TVal _value;

      internal KeyValItem(string key, TVal value, ItemDef type)
      {
         this.Key = key;      //e.g. IDCD_ID
         this._value = value;  //e.g. 71941
         this.ItemDef = type;

      }  // ctor


      /// <summary>
      /// Key (aka Field Name)
      /// </summary>
      public string Key { get; private set; }


      /// <summary>
      /// Value (strongly typed although of object type)
      /// </summary>
      public object Value
      {
         get { return _value; }
      }


      /// <summary>
      /// String representation of the item's value.
      /// </summary>
      public string StringValue
      {
         get
         {
            //TODO: Catch exceptions (such as FormatException in case _type.Format is wrong)

            //TODO: Increase functionality of Format strings.  Note that format strings may be too limiting in some situations.
            //      Example: how to make integers right-aligned (1 becomes "001" if Format="000" or "1" if Format="##0", but how to make it "  1"? - this may be needed for flat output)
            //      Consider changing ToString by string.Format and use composite formating, e.g. string.Format("{0,3:##0}",1) turns 1 into "  1"

            //Note: this is ugly code involving "double casting via object"
            //TODO: Consider removing generic TVal type from KeyValItem (!) - use object instead(?)
#pragma warning disable IDE0041 // Use 'is null' check
            if (ReferenceEquals(_value, null)) return null;  // _value == null causes "RCS1165 Unconstrained type parameter checked for null."
#pragma warning restore IDE0041 // Use 'is null' check
            if (_value is string) return _value as string;
            if (ItemDef.Format == null) return _value.ToString();
            switch (ItemDef.Type)
            {
               case ItemType.Int:
                  Debug.Assert(_value is int);
                  return ((int)(object)_value).ToString(ItemDef.Format);

               case ItemType.DateTime:
                  Debug.Assert(_value is DateTime);
                  return ((DateTime)(object)_value).ToString(ItemDef.Format);

               case ItemType.Decimal:
                  Debug.Assert(_value is Decimal);
                  return ((Decimal)(object)_value).ToString(ItemDef.Format);

               case ItemType.String:
                  Debug.Assert(_value is string);
                  return _value as string;

               default: //Void
                  return null;  //"impossible" condition as Void type should never be defined for a key
            }

         }
      }


      /// <summary>
      /// Definition of the item type
      /// </summary>
      public ItemDef ItemDef { get; private set; }


      /// <summary>
      /// Strongly typed value of the proper type
      /// </summary>
      TVal ITypedItem<TVal>.Value
      {
         get { return _value; }
      }
   }


   /// <summary>
   /// This static KeyValItem class is a factory to instantiate strong-typed KeyValItem objects
   /// </summary>
   internal static class KeyValItem
   {
      /// <summary>
      /// Constructs the KeyValItem from a value of already intended type
      /// </summary>
      /// <param name="key"></param>
      /// <param name="value"></param>
      /// <param name="typeDefinitions"></param>
      /// <returns></returns>
      internal static IItem CreateItem(string key, object value, TypeDefinitions typeDefinitions)
      {
         //this overload avoids value casting in case of matching type
         return typeDefinitions.GetFldCreatorEx(key)(value);
      }
      /// <summary>
      /// Constructs the KeyValItem from a string value
      /// </summary>
      /// <param name="key"></param>
      /// <param name="value"></param>
      /// <param name="typeDefinitions"></param>
      /// <returns></returns>
      internal static IItem CreateItem(string key, string value, TypeDefinitions typeDefinitions)
      {
         //this overload takes string value and parses it into the type specified in typeDefinitions
         return typeDefinitions.GetFldCreator(key)(value);
      }

   }

}
