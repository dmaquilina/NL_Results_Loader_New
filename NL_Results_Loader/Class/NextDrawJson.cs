using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NL_Results_Loader.Class
{
    class NextDrawJson
    {
        public string gameName { get; set; }
        public string brandName { get; set; }
        public int id { get; set; }
        public string status { get; set; }
        public int drawCdc { get; set; }
        public long closeTime { get; set; }
        public long drawTime { get; set; }
        public bool wagerAvailable { get; set; }
        public bool cancelAvailable { get; set; }

    }
}
