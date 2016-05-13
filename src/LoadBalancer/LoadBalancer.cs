using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub
{
    [Serializable]
    public class LoadBalancer
    {
        private List<string> lstProcessos; //lista de processos 

        public LoadBalancer(List<string> list)
        {
            lstProcessos = list;

        }

        public List<string> LstProcessos
        {
            get
            {
                return lstProcessos;
            }

            set
            {
                lstProcessos = value;
            }
        }

        public string getTarget()
        {
            Random rnd = new Random();
            if(lstProcessos.Count > 0)
            {
                int n = rnd.Next(0, lstProcessos.Count - 1);

                string target = lstProcessos[n];
                return target;

            }
            else
            {
                return null;
            }

        }


    }
}
