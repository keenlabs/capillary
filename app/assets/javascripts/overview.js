var React = require('react');

var Topology = require('./topology');

var Overview = React.createClass({
  render: function() {
    var topologies = [];
    this.props.topologies.forEach(function(topo) {
      topologies.push(<Topology key={topo.name} name={topo.name} root={topo.root} topic={topo.topic}/>);
    });

    return (
      <table className="table table-striped">
        <thead>
          <tr>
            <th>Name</th>
            <th>Delta</th>
            <th>Topic</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {topologies}
        </tbody>
      </table>
    );
  }
});

// Just hitch this onto the window!
window.renderOverview = function(topologies, node) {
  React.render(<Overview fetchInterval={30000} topologies={topologies}/>, node);
}

module.exports = Overview;
