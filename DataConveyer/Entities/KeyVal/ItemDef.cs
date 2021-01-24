//ItemDef.cs
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


//Contents:
//  - enum ItemType
//  - struct ItemDef

using System;

namespace Mavidian.DataConveyer.Entities.KeyVal
{
   /// <summary>
   /// Possible data types of item values.
   /// </summary>
   [Flags]
   public enum ItemType
   {
      /// <summary>
      /// Represents a non-existent item, never assigned to an actual item.
      /// This type may be present in fixed size records, e.g. in case of <see cref="Common.KindOfTextData.Flat"/> data.
      /// </summary>
      Void = 1,
      /// <summary>
      /// String type.
      /// </summary>
      String = 2,
      /// <summary>
      /// Boolean type.
      /// </summary>
      Bool = 4,
      /// <summary>
      /// Integer type.
      /// </summary>
      Int = 8,
      /// <summary>
      /// Decimal type (money).
      /// </summary>
      Decimal = 16,
      /// <summary>
      /// Date type.
      /// </summary>
      DateTime = 32
   }

   /// <summary>
   /// Defines characteristics of an item.
   /// </summary>
   public struct ItemDef
   {
      /// <summary>
      /// Item type, one of: Bool, Int, Decimal, DateTime or String.
      /// </summary>
      public readonly ItemType Type;
      /// <summary>
      /// Output format string (the same as used in Format statement).
      /// </summary>
      public readonly string Format;  //for output only

      /// <summary>
      /// Constructs the item type definition for a given type and format.
      /// </summary>
      /// <param name="type">Type of the item to construct</param>
      /// <param name="format">Format of the item to construct</param>
      public ItemDef(ItemType type, string format)
      {
         Type = type;
         Format = format;
      }
   }

}
