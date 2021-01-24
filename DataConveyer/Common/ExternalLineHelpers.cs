//ExternalLineHelpers.cs
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
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Common
{
   /// <summary>
   /// Helper methods such as extension methods to assist in translating to/from <see cref="ExternalLine"/> objects.
   /// </summary>
   public static class ExternalLineHelpers
   {
      //These methods are used during LineFeeder (Intake) and LineDispenser (Output) processing.
      //Also exposed for public use (intake suppliers and output consumers).

#region Translations from string (Xtext)

      /// <summary>
      /// Translate a line of text into a tuple used by suppliers and consumers.
      /// </summary>
      /// <param name="line">A line of text.</param>
      /// <param name="indexNo">Either source number or target number.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and a source or target number.</returns>
      public static Tuple<ExternalLine, int> ToExternalTuple(this string line, int indexNo)
      {
         return Tuple.Create(line.ToExternalLine(), indexNo);
      }


      /// <summary>
      /// Translate a line of text into a tuple used by intake suppliers and output consumers in case of a single source or target.
      /// </summary>
      /// <param name="line">A line of text.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and a source or target number equal to 1.</returns>
      public static Tuple<ExternalLine, int> ToExternalTuple(this string line)
      {
         return line.ToExternalTuple(1);
      }


      /// <summary>
      /// Translate a line of text into a tuple consisting of this line and a corresponding source or target number.
      /// </summary>
      /// <param name="line">A line of text</param>
      /// <param name="indexNo">Either source or target number to assign.</param>
      /// <returns>A tuple consisting of the same line and a corresponding source or target number.</returns>
      public static Tuple<string, int> ToTuple(this string line, int indexNo)
      {
         return Tuple.Create(line, indexNo);
      }


      /// <summary>
      /// Translate a line of text into a tuple consisting of this line and a source or target number of 1.
      /// </summary>
      /// <param name="line">A line of text.</param>
      /// <returns>A tuple consisting of the same line and a source or target number of 1</returns>
      public static Tuple<string, int> ToTuple(this string line)
      {
         return line.ToTuple(1);
      }


      /// <summary>
      /// Translate a line of text into an <see cref="ExternalLine"/> object.
      /// </summary>
      /// <param name="line">A line of text.</param>
      /// <returns>An <see cref="ExternalLine"/> object of <see cref="Xtext"/> type.</returns>
      public static ExternalLine ToExternalLine(this string line)
      {
         return ExternalLine.CreateXtext(line);
      }

#endregion Translations from string (Xtext)


#region Translations from sequence of KVP (Xrecord)

      /// <summary>
      /// Translate a record expressed as a sequence of key-value pairs into a tuple used by intake suppliers and output consumers.
      /// </summary>
      /// <param name="record">A sequence of key-value pairs.</param>
      /// <param name="clstrNo">Cluster number; may be 0, which means undetermined.</param>
      /// <param name="indexNo">Either source number or target numbe.r</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and a source or target number.</returns>
      public static Tuple<ExternalLine, int> ToExternalTuple(this IEnumerable<Tuple<string, object>> record, int clstrNo, int indexNo)
      {
         return Tuple.Create(record.ToExternalLine(clstrNo), indexNo);
      }


      /// <summary>
      /// Translate a record expressed as a sequence of key-value pairs into a tuple used by by intake suppliers and output consumers in case of a single source or target.
      /// </summary>
      /// <param name="record">A sequence of key-value pairs.</param>
      /// <param name="clstrNo">Cluster number of the record; may be 0, which means undetermined.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and a source or target number equal to 1.</returns>
      public static Tuple<ExternalLine, int> ToExternalTuple(this IEnumerable<Tuple<string, object>> record, int clstrNo)
      {
         return record.ToExternalTuple(clstrNo, 1);
      }


      /// <summary>
      /// Translate a record expressed as a sequence of key-value pairs (where a cluster number is undetermined) into a tuple used by intake suppliers and output consumers in case of a single source or target.
      /// </summary>
      /// <param name="record">A sequence of key-value pairs.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object with an undetermined cluster number and a source or target number equal to 1.</returns>
      public static Tuple<ExternalLine, int> ToExternalTuple(this IEnumerable<Tuple<string, object>> record)
      {
         return record.ToExternalTuple(0, 1);
      }


      /// <summary>
      /// Translate a record expressed as a sequence of key-value pairs into an <see cref="ExternalLine"/> object.
      /// </summary>
      /// <param name="record">A sequence of key-value pairs.</param>
      /// <param name="clstrNo">Cluster number the record is associated with; may be 0, which means undetermined.</param>
      /// <returns>An <see cref="ExternalLine"/> object of type <see cref="Xrecord"/>.</returns>
      public static ExternalLine ToExternalLine(this IEnumerable<Tuple<string, object>> record, int clstrNo)
      {
         return record == null ? null : new Xrecord(record.ToList(), clstrNo);
      }


      /// <summary>
      /// Translate a record expressed as  a sequence of key-value pairs into an <see cref="ExternalLine"/> object with an undetermined cluster number.
      /// </summary>
      /// <param name="record">A sequence of key-value pairs.</param>
      /// <returns>An <see cref="ExternalLine"/> object of type <see cref="Xrecord"/>.</returns>
      public static ExternalLine ToExternalLine(this IEnumerable<Tuple<string, object>> record)
      {
         return record.ToExternalLine(0);
      }

#endregion Translations from KVP (Xrecord)


#region Translations from ExternalLine

      /// <summary>
      /// Translate an <see cref="ExternalLine"/> object into a tuple used by intake suppliers and output consumers.
      /// </summary>
      /// <param name="line">An <see cref="ExternalLine"/> object.</param>
      /// <param name="indexNo">Either source or target number to assign.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and a source or target number.</returns>
      public static Tuple<ExternalLine, int> ToTuple(this ExternalLine line, int indexNo)
      {
         return line == null ? null : Tuple.Create(line, indexNo);
      }


      /// <summary>
      /// Translate an <see cref="ExternalLine"/> object into a tuple used by intake suppliers and output consumers in case of a single source or target.
      /// </summary>
      /// <param name="line">An <see cref="ExternalLine"/> object.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and a source or target number of 1.</returns>
      public static Tuple<ExternalLine, int> ToTuple(this ExternalLine line)
      {
         return line.ToTuple(1);
      }

#endregion Translations from ExternalLine


#region Translations from Xtext tuple

      /// <summary>
      /// Translate a tuple with a line of text to a tuple with an <see cref="ExternalLine"/> object.
      /// </summary>
      /// <param name="txtTuple">A tuple consisting of a line of text and a source or target number.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and the same source or target number.</returns>
      public static Tuple<ExternalLine, int> ToExternalTuple(this Tuple<string, int> txtTuple)
      {
         if (txtTuple == null) return null;
         var text = txtTuple.Item1;
         var indexNo = txtTuple.Item2;  //either source or target number
         return text == null ? new Tuple<ExternalLine, int>(null, indexNo)
                             : Tuple.Create(text.ToExternalLine(), indexNo);
      }

#endregion Translations from Xtext tuple


#region Translations from Xrecord tuple

      /// <summary>
      /// Translate a tuple with a record (expressed as a sequence of key-value pairs) to a tuple with an <see cref="ExternalLine"/> object.
      /// </summary>
      /// <param name="recTuple">A tuple consisting of a record expressed as a sequence of key-value pairs and a source or target number.</param>
      /// <returns>A tuple consisting of an <see cref="ExternalLine"/> object and the same source or target number.</returns>
      public static Tuple<ExternalLine, int> ToExternalTuple(this Tuple<IEnumerable<Tuple<string, object>>, int> recTuple)
      {
         if (recTuple == null) return null;
         var rec = recTuple.Item1;
         var indexNo = recTuple.Item2;  //either source or target number
         return rec == null ? new Tuple<ExternalLine, int>(null, indexNo)
                             : Tuple.Create(rec.ToExternalLine(), indexNo);
      }

#endregion Translations from Xrecord tuple


#region Translations from ExternalLine tuple

      /// <summary>
      /// Translate a tuple with an <see cref="ExternalLine"/> object to a tuple with the same object and a source or target number of 1. 
      /// </summary>
      /// <param name="extTuple">An <see cref="ExternalLine"/> object along with corresponding source or target number.</param>
      /// <returns>The same <see cref="ExternalLine"/> object along with a source or target number of 1.</returns>
      public static Tuple<ExternalLine, int> StripIndexNo(Tuple<ExternalLine, int> extTuple)
      {
         return Tuple.Create(extTuple.Item1, 1);
      }

      /// <summary>
      /// Translate a tuple with an <see cref="ExternalLine"/> object to a tuple with a line of text.
      /// The source or output number remains unchanged.
      /// </summary>
      /// <param name="extTuple">An <see cref="ExternalLine"/> object along with corresponding source or target number.</param>
      /// <returns>A tuple with a line of text or null if parameter is null or tuple with null if parameter does not contain a line of text.</returns>
      public static Tuple<string, int> ToTextTuple(this Tuple<ExternalLine, int> extTuple)
      {
         if (extTuple == null) return null;
         var line = extTuple.Item1;
         var indexNo = extTuple.Item2;
         return line == null ? new Tuple<string, int>(null, indexNo)
                             : Tuple.Create(line.Text, indexNo);
      }


      /// <summary>
      /// Translate tuple with an <see cref="ExternalLine"/> object to a tuple with a record (expressed as a sequence of key-value pairs).
      /// The source or output number remains unchanged.
      /// </summary>
      /// <param name="extTuple">An <see cref="ExternalLine"/> object along with corresponding source or target number.</param>
      /// <returns>A tuple with a record or null if parameter is null or tuple with null if parameter does not contain a line of text.</returns>
      public static Tuple<IEnumerable<Tuple<string, object>>, int> ToRecordTuple(this Tuple<ExternalLine, int> extTuple)
      {
         if (extTuple == null) return null;
         var line = extTuple.Item1;
         var indexNo = extTuple.Item2;
         return line == null ? new Tuple<IEnumerable<Tuple<string, object>>, int>(null, indexNo)
                             : Tuple.Create(line.Items.AsEnumerable(), indexNo);
      }

#endregion Translations from ExternalLine tuple

   }
}