using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub
{
    [Serializable]
    public class TreeNode
    {

        //private HashSet<Subscriber> lstSubs;
        //private HashSet<Publisher> lstPubs;
        //private Broker broker;

        public Dictionary<string, string> vizinhos;//nomeBroker-{site;url}
        public string ID;
        private List<TreeNode> _children;
        public TreeNode Parent { get; private set; }

        public TreeNode(string id)
        {
            vizinhos = new Dictionary<string,string>();
            _children = new List<TreeNode>();
            //lstSubs = new HashSet<Subscriber>();
            //lstPubs = new HashSet<Publisher>();
            this.ID = id;

        }

        public Dictionary<string,string> getVizinhos() {
            return vizinhos;
        }

        //public Broker getBroker() { return broker; }

        //public void setBroker(Broker b) { broker = b; }

        //public HashSet<Publisher> getPubs() { return lstPubs; }

        //public HashSet<Subscriber> getSubs() { return lstSubs; }

        //public void addSubscriber(Subscriber s) { lstSubs.Add(s); }

        //public void addPublisher(Publisher p) { lstPubs.Add(p); }

        public List<TreeNode> GetChildren()
        {
            return this._children;
        }

        public TreeNode getChild(string id)
        {
            foreach (var child in _children)
            {
                if (child.ID.Equals(id))
                {
                    return child;
                }
            }
            return null;
        }

        public void AddChild(TreeNode item)
        {
            item.Parent = this;
            this._children.Add(item);
        }

        private void removeChild(TreeNode item)
        {
            this._children.Remove(item);
        }

        public int Count
        {
            get { return this._children.Count; }
        }

        public TreeNode FindID(string stringToFind)
        {
            // find the string, starting with the current instance
            return Find(this, stringToFind);
        }

        // Search for a string in the specified node and all of its children
        public TreeNode Find(TreeNode node, string stringToFind)
        {
            if (node.ID.Equals(stringToFind))
            {
                return node;
            }
            foreach (var child in node._children)
            {
                var result = Find(child, stringToFind);
                if (result != null)
                    return result;
            }

            return null;
        }


    }
}
