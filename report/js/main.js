hljs.highlightAll();
let divElement = document.getElementById('viz1625857280693');
let vizElement = divElement.getElementsByTagName('object')[0];
vizElement.style.width='100%';
vizElement.style.height=(divElement.offsetWidth*0.75)+'px';
let scriptElement = document.createElement('script');
scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
vizElement.parentNode.insertBefore(scriptElement, vizElement);

let divElement2 = document.getElementById('viz1625958651013');
let vizElement2 = divElement2.getElementsByTagName('object')[0];
vizElement2.style.width='100%';
vizElement2.style.height=(divElement2.offsetWidth*0.75)+'px';
let scriptElement2 = document.createElement('script');
scriptElement2.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
vizElement2.parentNode.insertBefore(scriptElement2, vizElement2);