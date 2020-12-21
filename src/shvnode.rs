use chainpack::metamethod::MetaMethod;
use chainpack::rpcvalue::List;
use chainpack::RpcValue;
use tracing::debug;

pub trait ShvNode<'a> {
    type MethodIterator: Iterator<Item=&'a MetaMethod>;
    type ChildrenIterator: Iterator<Item=&'a (&'a str, Option<bool>)>;

    fn methods(&'a self) -> Self::MethodIterator;
    fn children(&'a self) -> Self::ChildrenIterator;

    fn dir(&'a self, method_pattern: &str, attrs_pattern: u32) -> List {
        debug!("dir method pattern: {}, attrs pattern: {}", method_pattern, attrs_pattern);
        let mut lst: List = Vec::new();
        for mm in self.methods() {
            if method_pattern.is_empty() {
                lst.push(mm.dir_attributes(attrs_pattern as u8));
            }
            else if method_pattern == mm.name {
                lst.push(mm.dir_attributes(attrs_pattern as u8));
                break;
            }
        }
        debug!("dir: {:?}", lst);
        return lst;
    }
    fn ls(&'a self, name_pattern: &str, with_children_info: bool) -> List {
        debug!("ls name_pattern: {}, with_children_info: {}", name_pattern, with_children_info);
        let mut lst: List = Vec::new();
        let filter = |it: &'_ &'a (&'a str, Option<bool>)| {
            if !name_pattern.is_empty() {
                name_pattern == it.0
            } else {
                true
            }
        };
        let map = |it: &'a (&'a str, Option<bool>)| {
            if with_children_info {
                let mut lst = List::new();
                lst.push(RpcValue::new(it.0));
                match it.1 {
                    Some(b) => lst.push(RpcValue::new(b)),
                    None => lst.push(RpcValue::new(())),
                }
                RpcValue::new(lst)
            } else {
                RpcValue::new(it.0)
            }
        };
        let lst: List = self.children().filter(filter).map(map).collect();
        debug!("dir: {:?}", lst);
        return lst;
    }
}