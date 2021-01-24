//TraceableAsserter.cs
//
// Copyright © 2019-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System.IO;
using System.Linq;

namespace DataConveyer.Tests.TestHelpers
{
   //It would've been nice here to applye a "using alias directive" to define the type for the (Ext,Header,Formatter,ExcFormatter) tuple, e.g.:
   // using AsserterOutput<T> = ValueTuple<string, string, Func<T, string>, Func<Exception, IEnumerable<string>>>;
   //Unfortunaly, genertic types cannot be defined using this "using alias directive" ().
   //Besides, even without generics, it could've only be defined as an unnamed tuple, i.e. (Item1,Item2,Item3,Item4) instead of (Ext,Header,Formatter,ExcFormatter)

   /// <summary>
   /// Facilitates execution of asserts with output to a text file (or series of files)
   /// in case of assert failure.
   /// </summary>
   /// <typeparam name="T"></typeparam>
   internal class TraceableAsserter<T>
   {
      private readonly string _location;  //destination folder where the text file(s) will be saved (must end with a backslash unless empty), e.g. AsserterTestFailures\

      //A list of text output definiitions. Each definition is a tuple containing:
      // Ext          - file name extension (e.g. .txt
      // Header       - header line
      // Formatter    - translates a single instance (of T) into an output line
      // ExcFormatter - translates XunitException into a series of output lines
      private readonly List<(string Ext, string Header, Func<T, string> Formatter, Func<Exception, IEnumerable<string>> ExcFormatter)> _outputs;

      internal TraceableAsserter(string location, params (string Ext, string Header, Func<T, string> Formatter, Func<Exception, IEnumerable<string>> ExcFormatter)[] outputs)
      {
         _location = location;
         _outputs = new List<(string Ext, string Header, Func<T, string> Formatter, Func<Exception, IEnumerable<string>> ExcFormatter)>(outputs);
      }

      /// <summary>
      /// Execute (a series of) asserts over data provided (which is a list of some objects to assert on).
      /// In case any assert fails, the submitted data is saved in a text file (or series of files as configured in _outputs);
      /// after doing so, the assert failed exception (XunitException) is re-thrown to return control to the calling method.
      /// </summary>
      /// <param name="title">Contents of the first output row.</param>
      /// <param name="dataToAssertOn"></param>
      /// <param name="assertsToExecute"></param>
      internal void ExecuteAssertsWithSaveOnFailure(string title, List<T> dataToAssertOn, Action<List<T>> assertsToExecute)
      {
         try
         {
            assertsToExecute(dataToAssertOn);
         }
         catch (Exception ex)
         {
            //note that the location folder must exist; otherwise, assert failures will get superseded by System.IO.DirectoryNotFoundException
            var fileName = _location + Guid.NewGuid().ToString();
            _outputs.ForEach(o => File.WriteAllLines(fileName + o.Ext, dataToAssertOn.Select(d => o.Formatter(d)).Prepend(o.Header).Prepend(title).Concat(o.ExcFormatter(ex))));
            throw;
         }
      }

   }
}
