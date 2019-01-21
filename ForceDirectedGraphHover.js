function find_flow_by_recursion(nodesArray, data, nodes_found_index){
	    let next_node_pool = new Set();
	    
	    for (let i = nodesArray.length; i--;){
	        let index = data.nodes.findIndex(dict => dict.id === nodesArray[i].id);
	        //console.log('index', index);
	        nodes_found_index.add(index);

	        let next_node_index = new Array();
	        for (let j = data.links.length; j--;){
	            if (index == data.links[j].source){
	                next_node_index.push(data.links[j].target);
	            }
	        }
	        for (let i = next_node_index.length; i--; ){
	            next_node_pool.add(next_node_index[i]);
	        }
	    
	    }
	    let next_round = new Set([...next_node_pool].filter(x => !nodes_found_index.has(x)));
	    let array = Array.from(next_round);

	    if (array.length > 0){
	        let next_nodes = new Array();
	        for (let i = array.length; i--;){
	            next_nodes.push(data.nodes[array[i]]);
	        }
	        return next_nodes;
	    }
	    else {
	        return [];
	    }
	    
	}  

function find_nodes(startNode, data){
    var nodes_found_index = new Set();
    var next_nodes = new Array();
    var cnt = 0;

    next_nodes = find_flow_by_recursion(startNode, data, nodes_found_index);
    //console.log(cnt, next_nodes.length);

    while(next_nodes.length > 0){
        //console.log('enter the loop!')
        cnt = cnt+1;
        next_nodes = find_flow_by_recursion(next_nodes, data, nodes_found_index);
        //console.log(cnt, next_nodes);
    }
    //console.log('findres:', Array.from(nodes_found_index));
    return Array.from(nodes_found_index);
}

function find_links_first_round(startNodeIndex, dataset){
	var links_index = new Array();

	//var startNodeIndex = dataset.nodes.findIndex(dict => dict.id == startNodeID);
	
	for (let i = dataset.links.length; i--;){
		if (dataset.links[i].source == startNodeIndex){
			links_index.push([i, dataset.links[i].target]);
		}
	}

	//console.log('first round', links_index);
	return links_index;
}

function find_links_by_recursion(linksArray, dataset, links_found_index){
	var next_links_pool = new Set();
	
	for (let i = linksArray.length; i--;){
		links_found_index.add(linksArray[i][0]);

		for (let j = dataset.links.length; j--;){
			if(dataset.links[j].source == linksArray[i][1]){
				//console.log(dataset.links[j].source, linksArray[i][1])
				next_links_pool.add(j)
			}
		}
	}
	let next_round = new Set([...next_links_pool].filter(x => !links_found_index.has(x)));
	let array = Array.from(next_round);
	if (array.length > 0){
		next_links = new Array();
		for(let i = array.length; i--;){
			next_links.push([array[i], dataset.links[array[i]].target]);
		}
		return next_links;
	} 
	else {
		return [];
	}
}

function find_links(startNodeID, dataset){
	var links_found_index = new Set();
	var next_links = new Array();

	next_links = find_links_first_round(startNodeID, dataset);
	var cnt =0;
	while(next_links.length > 0){
		//console.log('enter the loog!');
		next_links = find_links_by_recursion(next_links, dataset, links_found_index);
		cnt = cnt+1;
		//console.log(cnt, next_links);
	}

	return Array.from(links_found_index);
}

var ForceDirectedGraph = {

	render: function(dataset){
		var w = 1600;
	    var h = 1200;
	    var linkDistance=100;

	    var colors = d3.scale.category10();

	    var allNodes = {...dataset};

	    //var test = find_links(4, allNodes);

	    var node_classes = new Array();
	    var link_classes = new Array();

	    var exchanges = ['unknown',
						 'Nami ICO',
						 'HitBTC',
						 'BNB',
						 'Globitex Token',
						 'Coinex',
						 'ETERBASE',
						 'Gemini',
						 'Kraken',
						 'Mothership',
						 'COSS',
						 'JEX Token',
						 'Yunbi',
						 'NEXT.exchange',
						 'EthBits ETBS Token',
						 'Bitbox',
						 'Golix Token',
						 'Cobinhood',
						 'MetaMorph',
						 'Hotbit Token',
						 'ShapeShift',
						 'ClassyCoin',
						 'Bittrex',
						 'Kryptono',
						 'FUTURAX',
						 'Allcoin',
						 'Upbit',
						 'Ethershift',
						 'AllBit',
						 'BitUN.io',
						 'Gate.io',
						 'CoinExchange.io',
						 'Poloniex',
						 'Bitibu',
						 'Bgogo',
						 'Kucoin',
						 'LocalCoinSwap Cryptoshare',
						 'Bitmax',
						 'BitMartToken',
						 'ZBToken',
						 'Changelly',
						 'Cryptopia',
						 'CGCX Exchange Token',
						 'Bibox',
						 'Tokenize Emblem',
						 'AXNET',
						 'Remitano',
						 'Coinolix token',
						 'BigONE',
						 'Bilaxy',
						 'Bitfinex',
						 'Dynamic Trading Rights',
						 'Bithemoth',
						 'KubitCoin',
						 'Huobi',
						 'Bitlim Token',
						 'CapdaxToken',
						 'Mercatox',
						 'Hotbit',
						 'BTCCToken',
						 'ABCC Token',
						 'Kodobit',
						 'Okex',
						 'KNOW',
						 'Uex',
						 'Trade',
						 'INCX Coin',
						 'FCoin',
						 'Digital Asset Exchange Token',
						 'CryptalDash',
						 'Coindelta',
						 'Liqui.io',
						 'Binance',
						 'Coinbene']; 

	    for (let i = 0; i < allNodes.nodes.length; i++){
	    	//console.log(allNodes.nodes[i].id);
	    	node_classes.push(find_nodes([allNodes.nodes[i]], allNodes));
	    	//console.log(node_classes[i]);

	    	link_classes.push(find_links(i, allNodes));
	    }
	 
	 	d3.select("svg").remove();
	    var svg = d3.select("body").append("svg").attr({"width":w,"height":h});	    

	    var force = d3.layout.force()
	        .nodes(dataset.nodes)
	        .links(dataset.links)
	        .size([w,h])
	        .linkDistance([linkDistance])
	        .charge([-500])
	        .theta(0.1)
	        .gravity(0.05)
	        .start();

	    var edges = svg.selectAll("line")
	      .data(dataset.links)
	      .enter()
	      .append("line")
	      .attr("id",function(d,i) {return 'edge'+i})
	      .attr('marker-end','url(#arrowhead)')
	      .style("stroke","#ccc")
	      .style("pointer-events", "none")
	      .attr("strock-width", d => Math.sqrt(d.value/10000));

	      //console.log('edges', edges);
	    
	    var nodes = svg.selectAll("circle")
	      .data(dataset.nodes)
	      .enter()
	      .append("circle")
	      .attr({"r":6})
	      .attr("id", function(d,i){return 'node'+i})
	      .style("fill",function(d,i){return colors(d.group);})
	      .on("mouseover", mouseOver(.2))
	      .on("mouseout", mouseOut)
	      .on("mousedown", function(d){
	      	d3.select(nodelabels[0][d.index]).style("visibility", "visible");
	      	d3.select(this)
	        .transition()
	        .duration(1000)
	        .attr({"r": 11});	        
	      })	      
	      .on("mouseup", function(d){
	      	d3.select(nodelabels[0][d.index]).style("visibility", "hidden");
	      	d3.select(this)
	        .transition()
	        .duration(500)
	        .attr({"r":6});
	        
	      })
	      .call(force.drag);

	      // fade nodes on hover
	      function isConnected(d, o){
	      	let currentNodeIndex = allNodes.nodes.findIndex(dict => dict.id == d.id);
	      	let searchNodeIndex = allNodes.nodes.findIndex(dict => dict.id == o.id);
	      	if (node_classes[currentNodeIndex].indexOf(searchNodeIndex) == -1){
	      		return false;
	      	}
	      	else{
	      		return true;
	      	}
	      }

	      function mouseOver(opacity){

	      	return function(d){
	      		nodes.style("stroke-opacity", function(o){
	      			thisOpacity = isConnected(d, o) ? 1 : opacity;
	      			return thisOpacity;
	      		});
	      		nodes.style("fill-opacity", function(o){
	      			thisOpacity = isConnected(d, o) ? 1 : opacity;
	      			return thisOpacity;
	      		});
	      		// fades the links
	      		let currentNodeIndex = allNodes.nodes.findIndex(dict => dict.id == d.id);
	      		for(let i = link_classes[currentNodeIndex].length; i--;){
	      			d3.select("#edgepath"+ link_classes[currentNodeIndex][i]).attr("stroke-opacity", 1);
	      			d3.select("#edgepath" + link_classes[currentNodeIndex][i]).attr("fill-opacity", 1);
	      		}	      		
	      	}
	      }

	      function mouseOut(){
	      	nodes.style("stroke-opacity", 1);
        	nodes.style("fill-opacity", 1);
        	edgepaths.attr("stroke-opacity", 0);
        	edgepaths.attr("fill-opacity", 0);
	      }

	    var nodelabels = svg.selectAll(".nodelabel") 
	       .data(dataset.nodes)
	       .enter()
	       .append("text")
	       .attr({"x":function(d){return d.x;},
	              "y":function(d){return d.y;},
	              "class":"nodelabel",
	              "stroke":"black"})
	       .text(function(d){return d.id+ "_"+exchanges[d.group];})
	       .attr({"font-size": 11})
	       .style("visibility", "hidden");

	       //console.log('nodelabels', nodelabels)
	      

	    var edgepaths = svg.selectAll(".edgepath")
	        .data(dataset.links)
	        .enter()
	        .append('path')
	        .attr({'d': function(d) {return 'M '+d.source.x+' '+d.source.y+' L '+ d.target.x +' '+d.target.y},
	               'class':'edgepath',
	               'fill-opacity':0.7,
	               'stroke-opacity':0.7,
	               'fill':'blue',
	               'stroke':'lightblue',
	               'id':function(d,i) {return 'edgepath'+i},
	           	   'stroke-width': d => Math.sqrt(d.value/10000)})
	        .style("pointer-events", "none");

	    var edgelabels = svg.selectAll(".edgelabel")
	        .data(dataset.links)
	        .enter()
	        .append('text')
	        .style("pointer-events", "none")
	        .attr({'class':'edgelabel',
	               'id':function(d,i){return 'edgelabel'+i},
	               'dx':50,
	               'dy':0,
	               'font-size':12,
	               'fill':'#aaa'});

	    edgelabels.append('textPath')
	        .attr('xlink:href',function(d,i) {return '#edgepath'+i})
	        .style("pointer-events", "none")
	        .text(function(d,i){return d.value});
	        //.style("visibility", "hidden");

	        //console.log('what is edgelabel', edgelabels);


	    svg.append('defs').append('marker')
	        .attr({'id':'arrowhead',
	               'viewBox':'-0 -5 10 10',
	               'refX':25,
	               'refY':0,
	               //'markerUnits':'strokeWidth',
	               'orient':'auto',
	               'markerWidth':10,
	               'markerHeight':10,
	               'xoverflow':'visible'})
	        .append('svg:path')
	            .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
	            .attr('fill', '#ccc')
	            .attr('stroke','#ccc');
	     

	    force.on("tick", function(){

	        edges.attr({"x1": function(d){return d.source.x;},
	                    "y1": function(d){return d.source.y;},
	                    "x2": function(d){return d.target.x;},
	                    "y2": function(d){return d.target.y;}
	        });

	        nodes.attr({"cx":function(d){return d.x;},
	                    "cy":function(d){return d.y;}
	        });

	        nodelabels.attr("x", function(d) { return d.x; }) 
	                  .attr("y", function(d) { return d.y; });

	        edgepaths.attr('d', function(d) { var path='M '+d.source.x+' '+d.source.y+' L '+ d.target.x +' '+d.target.y;
	                                           //console.log(d)
	                                           return path});       

	        edgelabels.attr('transform',function(d,i){
	            if (d.target.x<d.source.x){
	                bbox = this.getBBox();
	                rx = bbox.x+bbox.width/2;
	                ry = bbox.y+bbox.height/2;
	                return 'rotate(180 '+rx+' '+ry+')';
	                }
	            else {
	                return 'rotate(0)';
	                }
	        });
	    });

	    //add legend
		// function get_legend(allNodes){
		// 	let groups = new Set();
		// 	for(let i = allNodes.nodes.length;i--;){
		// 		groups.add(allNodes.nodes[i].group)
		// 	}
		// 	return groups;
		// }
		// console.log('ex', exchanges);

		// var legend = svg.selectAll(".legend")
		//     .data(get_legend(allNodes))
		//     .enter().append("g")
		//     .attr("class", "legend")
		//     .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

		// legend.append("rect")
		//     .attr("x", width - 18)
		//     .attr("width", 18)
		//     .attr("height", 18)
		//     .style("fill", function(d){return colors(d)});

		// legend.append("text")
		//     .attr("x", width - 24)
		//     .attr("y", 9)
		//     .attr("dy", ".35em")
		//     .style("text-anchor", "end")
		//     .text(function(d) { return d; });
	}
}