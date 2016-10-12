/**
 * Created by rafaelkyrdan on 10/11/16.
 */

function LinkedListNode(value) {
    this.value = value;
    this.next = null;
}

var list = new LinkedListNode('a');
list.next = new LinkedListNode('b');
list.next.next = new LinkedListNode('c');
list.next.next.next = new LinkedListNode('d');

var node = reverseList(list);
while (node) {
    console.log(node.value);
    node = node.next ? node.next : null;
}

function reverseList(list) {
    var node = list;
    var prev = next = null;

    while(node) {
        next = node.next;
        node.next = prev;
        prev = node;
        node = next;
    }

    return prev;
}