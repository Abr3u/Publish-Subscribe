using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;

namespace PubSub
{
    class localPM
    {
        private static string proj_path = Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName).FullName;


        static void Main(string[] args)
        {
            RemotingConfiguration.Configure(proj_path + @"\localPM\App.config", false);
            Console.WriteLine("@localPM !!! porto -> 1000");

            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            provider.TypeFilterLevel = TypeFilterLevel.Full;
            IDictionary props = new Hashtable();
            props["port"] = 1000;
            TcpChannel channel = new TcpChannel(props, null, provider);
            
            ChannelServices.RegisterChannel(channel, false);

            PMcreateProcess createProcess = new PMcreateProcess(1000);
            RemotingServices.Marshal(createProcess, "PuppetMasterURL", typeof(PMcreateProcess));

            Console.ReadLine();
        }
    }

    class PMcreateProcess : MarshalByRefObject, PuppetInterface
    {
        int portCounter;
        private static string proj_path = Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName).FullName;

        public PMcreateProcess(int pC)
        {
            portCounter = pC+1;
        }


        public void createProcess(TreeNode site, string role, string name, string s, string url,List<string> urlReplicas, List<string> urlPai, string urlPup)
        {

            string aux = "LocalPMcreateProcess @ url -> " + url + " site -> " + s;
            Console.WriteLine(aux);

            if (role.Equals("brokerRoot"))
            {

                Console.WriteLine("Vou criar o broker ROOT");

                string[] z = url.Split(':');//z[0]->tcp;z[1]->//localhost;z[2]->XXXX/broker
                string[] y = z[2].Split('/');
                string port = y[0];

                string brokers = fillArgument(site);

                ProcessStartInfo startInfo = new ProcessStartInfo(proj_path + @"\Broker\bin\Debug\Broker.exe");

                name = name + "?root";

                string replicas = "";

                foreach(var r in urlReplicas)
                {

                        replicas += r + "#";
                }

                string[] args = { port, url, name, s, replicas , "null", urlPup, brokers };
                startInfo.Arguments = String.Join(";", args);

                Process p = new Process();
                p.StartInfo = startInfo;

                p.Start();
            }
            
            if (role.Equals("broker"))
            {

                string[] z = url.Split(':');//z[0]->tcp;z[1]->//localhost;z[2]->XXXX/broker
                string[] y = z[2].Split('/');
                string port = y[0];


                string brokers = fillArgument(site);

                string replicas = "";

                foreach(var r in urlReplicas)
                {

                        replicas += r + "#";
                }

                string pai ="";
                foreach(var a in urlPai){
                    pai+= a+"|";
                }


                ProcessStartInfo startInfo = new ProcessStartInfo(proj_path + @"\Broker\bin\Debug\Broker.exe");
                string[] args = { port, url, name, s, replicas, pai, urlPup, brokers };


                startInfo.Arguments = String.Join(";", args);

                Process p = new Process();
                p.StartInfo = startInfo;

                p.Start();


            }
            if (role.Equals("subscriber"))
            {
                string[] z = url.Split(':');//z[0]->tcp;z[1]->//localhost;z[2]->XXXX/broker
                string[] y = z[2].Split('/');
                string port = y[0];

                ProcessStartInfo startInfo = new ProcessStartInfo(proj_path + @"\Subscriber\bin\Debug\Subscriber.exe");
              
                string[] args = { port, url, name, s, urlPup };

                startInfo.Arguments = String.Join(";", args)+ ";" +String.Join(";",urlReplicas);

                Process p = new Process();
                p.StartInfo = startInfo;

                p.Start();
            }
            if (role.Equals("publisher"))
            {
                string[] z = url.Split(':');//z[0]->tcp;z[1]->//localhost;z[2]->XXXX/broker
                string[] y = z[2].Split('/');
                string port = y[0];

                ProcessStartInfo startInfo = new ProcessStartInfo(proj_path + @"\Publisher\bin\Debug\Publisher.exe");
                string[] args = { port, url, name, s, urlPup};
                startInfo.Arguments = String.Join(";", args) + ";" + String.Join(";", urlReplicas);

                Process pro = new Process();
                pro.StartInfo = startInfo;

                pro.Start();
            }

        }

        private string fillArgument(TreeNode site)
        {
            string res = "";
            foreach (var aux in site.getVizinhos()) {
                res += aux.Key + "%" + aux.Value+";";
            }
            return res;
        }
    }
}
