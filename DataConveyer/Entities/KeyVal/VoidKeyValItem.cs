//VoidKeyValItem.cs
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


namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Represents "non-existing" item, e.g. returned for a key that doesn't exist in a KeyValRecord
   /// </summary>
   internal class VoidKeyValItem : IItem
   {
      internal VoidKeyValItem(string key)
      {
         Key = key;
      }  //ctor

      /// <summary>
      /// Always returns Void type
      /// </summary>
      public ItemDef ItemDef
      {
         get { return new ItemDef(ItemType.Void, null); }
      }

      /// <summary>
      /// Key of the item (aka Field Name)
      /// </summary>
      public string Key { get; private set; }

      /// <summary>
      /// Value of the Void item is always null
      /// </summary>
      public object Value
      {
         get
         {
            return null;
         }
      }

      /// <summary>
      /// StringValue of the Void item is an empty string;
      /// </summary>
      public string StringValue
      {
         get
         {
            ////return "<<VOID>>";
            return string.Empty;
         }
      }


      /// <summary>
      /// Cloning void item returns the item itself
      /// </summary>
      /// <returns></returns>
      IItem Clone()
      {
         return this;
      }
      internal IItem Clone(string value, TypeDefinitions typeDefinitions)
      {
         //value is ignored for void item
         return this;
      }
      internal IItem Clone(object value, TypeDefinitions typeDefinitions)
      {
         //value is ignored for void item
         return this;
      }
   }
}
