//IOrchestrator.cs
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


namespace Mavidian.DataConveyer.Orchestrators
{
   /// <summary>
   /// A factory class to create orchestrator instances, i.e. objects that implement the <see cref="IOrchestrator"/> interface.
   /// </summary>
   public static class OrchestratorCreator
   {
      /// <summary>
      /// A factory method to create an instance of ETL orchestrator.
      /// </summary>
      /// <param name="config">Configuration parameters of ETL orchestrator to create.</param>
      /// <returns>The instance of ETL orchestrator (just created).</returns>
      public static IOrchestrator GetEtlOrchestrator(OrchestratorConfig config)
      {
         return new EtlOrchestrator(config);
      }

      //In the future, other Get..Orchestrator methods can be added (or GetEtlOrchestrator decorated
      // with parameters) to return different orchestrators that implement IOrchestrator interface.
   }
}
