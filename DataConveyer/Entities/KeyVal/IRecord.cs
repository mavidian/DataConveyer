//IRecord.cs
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
   /// Interface defining a record; it refers to strongly typed values of items contained in the record
   /// </summary>
   public interface IRecord : IRecordBase
   {
      /// <summary>
      /// A value of an item for a specified key.
      /// (attempt to set a value of a non-existing item has no effect)
      /// </summary>
      /// <param name="key">Key of the item</param>
      /// <returns>A typed item value or null if item does not exists.</returns>
      object this[string key] { get; set; }
      /// <summary>
      /// A value of an item at a specified index position.
      /// (attempt to set a value of a non-existing item has no effect)
      /// </summary>
      /// <param name="index">Index position of the item.</param>
      /// <returns>A typed item value or null if index is out of range.</returns>
      object this[int index] { get; set; }
      /// <summary>
      /// Add an item for a given key and value at the end of the record.
      /// </summary>
      /// <param name="key">The key of an item to add.</param>
      /// <param name="value">The value of an item to add.</param>
      /// <returns>The item just added or void item if key already existed (and thus no item was added); if item was not added because additions/removals are disallowed return null.</returns>
      IItem AddItem(string key, object value);
      /// <summary>
      /// Obtain a clone of a given item with a new value.
      /// </summary>
      /// <param name="item">An item to clone.</param>
      /// <param name="value">New value for the item.</param>
      /// <returns>An item with the same key and a new value; if a void item is passed, then void item is returned.</returns>
      IItem GetItemClone(IItem item, object value);
      /// <summary>
      /// Return a copy (deep clone) of the record.
      /// The cloned record will have the same characteristics (e.g. RecNo and also TraceBin and PropertyBin) as the current record.
      /// </summary>
      /// <returns>A clone of the current record.</returns>
      IRecord GetClone();
      /// <summary>
      /// <para>
      /// Return an empty record (i.e. record with no items) with the same characteristics (e.g. RecNo and also TraceBin and PropertyBin) as the current record.
      /// </para>
      /// <note type="caution">
      /// GetEmptyClone method typically requires AllowTransformToAlterFields setting to be true (to allow field additions).
      /// Otherwise (i.e. when AllowTransformToAlterFields is false, which is a default value), the record returned by this method is unmaintainable, i.e. it will remain empty forever.
      /// </note>
      /// </summary>
      /// <returns>An empty record (with no items).</returns>
      IRecord GetEmptyClone();
      /// <summary>
      /// Return an empty X12 segment for a given name and number of elements.
      /// </summary>
      /// <param name="name">Segment type (name), e.g. NM1.</param>
      /// <param name="elementCount">Number of elements in the segment (each element is assigned an empty string value).</param>
      /// <returns>An empty record (with the same RecNo as on current record) representing X12 segment.</returns>
      IRecord CreateEmptyX12Segment(string name, int elementCount);
      /// <summary>
      /// Return an X12 segment based on provided contents.
      /// </summary>
      /// <param name="contents">Contents of the entire segment with field delimiter between fields, but no segment delimiter at end.</param>
      /// <param name="fieldDelimiter">Field delimiter character used in contents; if omitted DefaultX12Field delimiter is used if any; otherwise * is assumed.</param>
      /// <returns>A record (with the same RecNo as on current record) representing the X12 segment.</returns>
      IRecord CreateFilledX12Segment(string contents, char fieldDelimiter = '\0');
   }
}
