using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NL_Results_Loader.Class
{
    class GameDrawJSON
    {
        public List<DrawJSON> draws { get; set; }
    }

    public class DrawJSON
    {
        public string gameName { get; set; }
        public string brandName { get; set; }
        public string id { get; set; }
        public string status { get; set; }
        public int drawCdc { get; set; }
        public long closeTime { get; set; }
        public long drawTime { get; set; }
        public bool wagerAvailable { get; set; }
        public bool cancelAvailable { get; set; }
        public int estimatedJackpot { get; set; }
        public JackpotJSON[] jackpots { get; set; }
        public ResultJSON[] results { get; set; }
        public PrizetierJSON[] prizeTiers { get; set; }
    }

    public class JackpotJSON
    {
        public int amount { get; set; }
        public int cashAmount { get; set; }
    }

    public class ResultJSON
    {
        public string[] primary { get; set; }
        public string prizeTierId { get; set; }
    }

    public class PrizetierJSON
    {
        public int shareCount { get; set; }
        public int shareAmount { get; set; }
        public string name { get; set; }
        public string id { get; set; }
        public string prizeType { get; set; }
    }
}
