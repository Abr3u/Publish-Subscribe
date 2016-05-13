using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PubSub
{
    static class PuppetMaster
    {
        //private static string proj_path = Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName).FullName;
        //private static string conf_filename = proj_path+@"\example.txt";
        private static string proj_path = Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName).FullName;

        static void Main(string[] args)
        {

            RemotingConfiguration.Configure(proj_path + @"\PuppetMaster\App.config", false);
            TcpChannel channel = new TcpChannel(Int32.Parse(args[0]));
            ChannelServices.RegisterChannel(channel, false);

            PMcreateProcess createProcess = new PMcreateProcess(Int32.Parse(args[0]));
            RemotingServices.Marshal(createProcess, "PuppetMasterURL", typeof(PMcreateProcess));

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new Form1(args[1]));



            
        }
    }

    class PMcreateProcess : MarshalByRefObject, PuppetInterface
    {

        // vida infinita !!!!
        public override object InitializeLifetimeService()
        {
            return null;
        }
        int portCounter;
        private static string proj_path = Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName).FullName;

        public PMcreateProcess(int pC)
        {
            portCounter = pC;
        }

        private string fillArgument(TreeNode site)
        {
            string res = "";
            foreach (var aux in site.getVizinhos())
            {
                res += aux.Key + "%" + aux.Value + ";";
            }
            return res;
        }

        public void createProcess(TreeNode site, string role, string name, string s, string url, List<string> urlReplicas, List<string> urlPai, string urlPup)
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

                foreach (var r in urlReplicas)
                {

                    replicas += r + "#";
                }

                string[] args = { port, url, name, s, replicas, "null", brokers };
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

                foreach (var r in urlReplicas)
                {

                    replicas += r + "#";
                }

                string pai = "";
                foreach (var a in urlPai)
                {
                    pai += a + "|";
                }


                ProcessStartInfo startInfo = new ProcessStartInfo(proj_path + @"\Broker\bin\Debug\Broker.exe");
                string[] args = { port, url, name, s, replicas, pai, brokers };


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

                string[] args = { port, url, name, s };

                startInfo.Arguments = String.Join(";", args) + ";" + String.Join(";", urlReplicas);

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
                string[] args = { port, url, name, s };
                startInfo.Arguments = String.Join(";", args) + ";" + String.Join(";", urlReplicas);

                Process pro = new Process();
                pro.StartInfo = startInfo;

                pro.Start();
            }

        }
    }

    class PMLog : MarshalByRefObject, LogInterface
    {

        // vida infinita !!!!
        public override object InitializeLifetimeService()
        {
            return null;
        }

        private static string proj_path = Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName).FullName;
        private static object myLock = new Object();

        private int mode;

        public PMLog(int m)
        {
            File.Create(proj_path+@"\log.txt").Close();
            this.mode = m;
        }

        public void scriptLog(string line) {
            if (!line.Equals(""))
            {
                lock (myLock)
                {
                    using (FileStream file = new FileStream(proj_path + @"\log.txt", FileMode.Append, FileAccess.Write, FileShare.Read))
                    using (StreamWriter writer = new StreamWriter(file, Encoding.Unicode))
                    {
                        writer.WriteLine(line);
                    }
                }

            }
        }

        public void log(string selfName, string pubName, string topicName, int eventNumber, string ID)
        {
            //MessageBox.Show("Estou a escrever no log, quem me chamou foi o " + selfName);
            string line = "";
            if (ID.Equals("publisher"))
            {
                line = "PubEvent " + selfName + ", " + pubName + ", " + topicName + ", " + eventNumber;
            }
            if (ID.Equals("broker"))
            {
                if (this.mode != 0)//se for full
                {
                    line = "BroEvent " + selfName + ", " + pubName + ", " + topicName + ", " + eventNumber;
                }
            }
            if (ID.Equals("subscriber"))
            {
                line = "SubEvent " + selfName + ", " + pubName + ", " + topicName + ", " + eventNumber;
            }
            if (!line.Equals(""))
            {
                lock (myLock)
                {
                    using (FileStream file = new FileStream(proj_path + @"\log.txt", FileMode.Append, FileAccess.Write, FileShare.Read))
                    using (StreamWriter writer = new StreamWriter(file, Encoding.Unicode))
                    {
                        writer.WriteLine(line);
                    }
                }
                
            }
        }
    }
}
