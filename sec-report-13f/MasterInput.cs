using System;
using System.Collections.Generic;

namespace MakeReport13F
{
    public class MasterInput
    {
        public List<string> Ids { get; set; }

        public int PageNumber { get; set; }

        public MasterInput(List<string> ids, int pageNumber)
        {
            Ids = ids;
            PageNumber = pageNumber;
        }
    }
}
