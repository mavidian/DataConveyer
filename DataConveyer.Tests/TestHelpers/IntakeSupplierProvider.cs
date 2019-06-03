//IntakeSupplierProvider.cs
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


using Mavidian.DataConveyer.Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DataConveyer.Tests.TestHelpers
{
   /// <summary>
   /// Helper class to provide various intake suppliers for a given sequence of text lines from intake
   /// </summary>
   internal class IntakeSupplierProvider
   {
      private readonly Queue<string> _intakeQueue;

      internal IntakeSupplierProvider(IEnumerable<string> intakeLines)
      {
         _intakeQueue = new Queue<string>(intakeLines);
      }

      internal Func<string> StringSupplier
      {
         get
         {
            return () =>
            {
               if (_intakeQueue.Count == 0) return null;
               return _intakeQueue.Dequeue();
            };
         }
      }

      internal Func<Tuple<string, int>> StringTupleSupplier
      {
         get
         {
            return () =>
            {
               if (_intakeQueue.Count == 0) return null;
               return _intakeQueue.Dequeue().ToTuple(); // SourceNo is 1
            };
         }
      }

      /// <summary>
      /// Returns an intake supplier function that supplies tuples with a string line and a source number.
      /// </summary>
      /// <param name="sourceNoEval">Function that returns the SourceNo to assign to currect record. It is called exactly once for each returned tuple.</param>
      /// <returns></returns>
      internal Func<Tuple<string, int>> GetStringTupleSupplier(Func<int> sourceNoEval)
      {
         return () =>
         {
            if (_intakeQueue.Count == 0) return null;
            return _intakeQueue.Dequeue().ToTuple(sourceNoEval());
         };
      }

      internal Func<ExternalLine> ExternalLineSupplier
      {
         get
         {
            return () =>
            {
               if (_intakeQueue.Count == 0) return null;
               return _intakeQueue.Dequeue().ToExternalLine(); // ExternalLineType is Xtext
            };
         }
      }
      internal Func<Tuple<ExternalLine,int>> ExternalTupleSupplier
      {
         get
         {
            return () =>
            {
               if (_intakeQueue.Count == 0) return null;
               return _intakeQueue.Dequeue().ToExternalTuple(); // ExternalLineType is Xtext, SourceNo is 1
            };
         }
      }


      //Asynchronous (simulated by returning tasks with result available immediately):

      internal Func<Task<string>> AsyncStringSupplier
      {
         get
         {
            return () =>
            {
               if (_intakeQueue.Count == 0) return Task.FromResult<string>(null);
               return Task.FromResult(_intakeQueue.Dequeue());
            };
         }
      }

      internal Func<Task<Tuple<ExternalLine, int>>> AsyncExternalTupleSupplier
      {
         get
         {
            return () =>
            {
               if (_intakeQueue.Count == 0) return Task.FromResult<Tuple<ExternalLine, int>>(null);
               return Task.FromResult(_intakeQueue.Dequeue().ToExternalTuple()); // ExternalLineType is Xtext, SourceNo is 1
            };
         }
      }

   }
}
