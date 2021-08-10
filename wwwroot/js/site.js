//Make sure jQuery has been loaded before app.js
if (typeof jQuery === "undefined") {
    throw new Error("DEDUPProcess requires jQuery");
}

if (!Array.prototype.remove) {
    Array.prototype.remove = function () {
        var searchElm, a = arguments, arrLen = a.length, arrIndex;
        while (arrLen && this.length) {
            searchElm = a[--arrLen];
            while ((arrIndex = this.indexOf(searchElm)) !== -1) {
                this.splice(arrIndex, 1);
            }
        }
        return this;
    };
}

$.DEDUPProcess = {};

/* --------------------
 * - DEDUPProcess Options -
 * --------------------
 * Modify these options to suit your implementation
 */
$.DEDUPProcess.options = {
    //Add slimscroll to navbar menus
    //This requires you to load the slimscroll plugin
    //in every page before app.js
    navbarMenuSlimscroll: true,
    navbarMenuSlimscrollWidth: "3px", //The width of the scroll bar
    navbarMenuHeight: "200px", //The height of the inner menu
    //General animation speed for JS animated elements such as box collapse/expand and
    //sidebar treeview slide up/down. This options accepts an integer as milliseconds,
    //'fast', 'normal', or 'slow'
    animationSpeed: 500,
    //Sidebar push menu toggle button selector
    sidebarToggleSelector: "[data-toggle='offcanvas']",
    //Activate sidebar push menu
    sidebarPushMenu: true,
    //Activate sidebar slimscroll if the fixed layout is set (requires SlimScroll Plugin)
    sidebarSlimScroll: true,
    //Enable sidebar expand on hover effect for sidebar mini
    //This option is forced to true if both the fixed layout and sidebar mini
    //are used together
    sidebarExpandOnHover: false,
    //BoxRefresh Plugin
    enableBoxRefresh: true,
    //Bootstrap.js tooltip
    enableBSToppltip: true,
    BSTooltipSelector: "[data-toggle='tooltip']",
    //Enable Fast Click. Fastclick.js creates a more
    //native touch experience with touch devices. If you
    //choose to enable the plugin, make sure you load the script
    //before DEDUPProcess's app.js
    enableFastclick: false,
    //Control Sidebar Tree views
    enableControlTreeView: true,
    //Control Sidebar Options
    enableControlSidebar: true,
    controlSidebarOptions: {
        //Which button should trigger the open/close event
        toggleBtnSelector: "[data-toggle='control-sidebar']",
        //The sidebar selector
        selector: ".control-sidebar",
        //Enable slide over content
        slide: true
    },
    //Box Widget Plugin. Enable this plugin
    //to allow boxes to be collapsed and/or removed
    enableBoxWidget: true,
    //Box Widget plugin options
    boxWidgetOptions: {
        boxWidgetIcons: {
            //Collapse icon
            collapse: 'fa-minus',
            //Open icon
            open: 'fa-plus',
            //Remove icon
            remove: 'fa-times'
        },
        boxWidgetSelectors: {
            //Remove button selector
            remove: '[data-widget="remove"]',
            //Collapse button selector
            collapse: '[data-widget="collapse"]'
        }
    },
    //Direct Chat plugin options
    directChat: {
        //Enable direct chat by default
        enable: true,
        //The button to open and close the chat contacts pane
        contactToggleSelector: '[data-widget="chat-pane-toggle"]'
    },
    //Define the set of colors to use globally around the website
    colors: {
        lightBlue: "#3c8dbc",
        red: "#f56954",
        green: "#00a65a",
        aqua: "#00c0ef",
        yellow: "#f39c12",
        blue: "#0073b7",
        navy: "#001F3F",
        teal: "#39CCCC",
        olive: "#3D9970",
        lime: "#01FF70",
        orange: "#FF851B",
        fuchsia: "#F012BE",
        purple: "#8E24AA",
        maroon: "#D81B60",
        black: "#222222",
        gray: "#d2d6de"
    },
    //The standard screen sizes that bootstrap uses.
    //If you change these in the variables.less file, change
    //them here too.
    screenSizes: {
        xs: 480,
        sm: 768,
        md: 992,
        lg: 1200
    }
};

Array.prototype.remove = function () {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);
        }
    }
    return this;
};

$.DEDUPProcess.sortByProperty = function (obj, propertyName, caseSensitive, orderBy) {
    var isAscending = orderBy || true;
    var property = propertyName;

    obj.sort(function (a, b) {
        var sortStatus = 0;
        var x = (isAscending ? a : b);
        var y = (isAscending ? b : a)

        if (caseSensitive) {
            if (x[property] < y[property]) {
                sortStatus = -1;
            } else if (x[property] > y[property]) {
                sortStatus = 1;
            }
        }
        else if (x[property] && y[property]) {
            if (x[property].toLowerCase() < y[property].toLowerCase()) {
                sortStatus = -1;
            } else if (x[property].toLowerCase() > y[property].toLowerCase()) {
                sortStatus = 1;
            }
        }

        return sortStatus;
    })
};

$.DEDUPProcess.groupBy = function (funcProp) {
    return this.reduce(function (acc, val) {
        (acc[funcProp(val)] = acc[funcProp(val)] || []).push(val);
        return acc;
    }, {});
};

/* ------------------
 * - Implementation -
 * ------------------
 * The next block of code implements DEDUPProcess's
 * functions and plugins as specified by the
 * options above.
 */
$(function () {
    "use strict";

    //hide tooltip after mouseleave
    $("body,a,img[alt]").on('mouseleave', function () {
        $(this).blur();
    });

    //Fix for IE page transitions
    $("body").removeClass("hold-transition");

    //Extend options if external options exist
    if (typeof DEDUPProcessOptions !== "undefined") {
        $.extend(true,
          $.DEDUPProcess.options,
          DEDUPProcessOptions);
    }

    //Easy access to options
    var o = $.DEDUPProcess.options;

    //Set up the object
    _init();

    //Activate the layout maker
    //$.DEDUPProcess.layout.activate();


    //Enable sidebar tree view controls
    if (o.enableControlTreeView) {
        $.DEDUPProcess.tree('.sidebar');
    }

    //Enable control sidebar
    //if (o.enableControlSidebar) {
    //    $.DEDUPProcess.controlSidebar.activate();
    //}

    //Add slimscroll to navbar dropdown
    if (o.navbarMenuSlimscroll && typeof $.fn.slimscroll !== 'undefined') {
        $(".navbar .menu").slimscroll({
            height: o.navbarMenuHeight,
            alwaysVisible: false,
            size: o.navbarMenuSlimscrollWidth
        }).css("width", "100%");
    }

    //Activate sidebar push menu
    if (o.sidebarPushMenu) {
        $.DEDUPProcess.pushMenu.activate(o.sidebarToggleSelector);
    }

    //Activate Bootstrap tooltip
    if (o.enableBSToppltip) {
        $('body').tooltip({
            selector: o.BSTooltipSelector,
            container: 'body'
        });
    }

    //Activate box widget
    //if (o.enableBoxWidget) {
    //    $.DEDUPProcess.boxWidget.activate();
    //}

    //Activate fast click
    if (o.enableFastclick && typeof FastClick !== 'undefined') {
        FastClick.attach(document.body);
    }

    //Activate direct chat widget
    if (o.directChat.enable) {
        $(document).on('click', o.directChat.contactToggleSelector, function () {
            var box = $(this).parents('.direct-chat').first();
            box.toggleClass('direct-chat-contacts-open');
        });
    }

    /*
     * INITIALIZE BUTTON TOGGLE
     * ------------------------
     */
    $('.btn-group[data-toggle="btn-toggle"]').each(function () {
        var group = $(this);
        $(this).find(".btn").on('click', function (e) {
            group.find(".btn.active").removeClass("active");
            $(this).addClass("active");
            e.preventDefault();
        });

    });
});

/* ----------------------------------
 * - Initialize the DEDUPProcess Object -
 * ----------------------------------
 * All DEDUPProcess functions are implemented below.
 */
function _init() {
    'use strict';
    /* Layout
     * ======
     * Fixes the layout height in case min-height fails.
     *
     * @type Object
     * @usage $.DEDUPProcess.layout.activate()
     *        $.DEDUPProcess.layout.fix()
     *        $.DEDUPProcess.layout.fixSidebar()
     */
    $.DEDUPProcess.layout = {
        activate: function () {
            var _this = this;
            _this.fix();
            _this.fixSidebar();
            $('body, html, .wrapper').css('height', 'auto');
            $(window, ".wrapper").resize(function () {
                _this.fix();
                _this.fixSidebar();
            });
        },
        fix: function () {
            // Remove overflow from .wrapper if layout-boxed exists
            $(".layout-boxed > .wrapper").css('overflow', 'hidden');
            //Get window height and the wrapper height
            var footer_height = $('.main-footer').outerHeight() || 0;
            var neg = $('.main-header').outerHeight() + footer_height;
            var window_height = $(window).height();
            var sidebar_height = $(".sidebar").height() || 0;
            //Set the min-height of the content and sidebar based on the
            //the height of the document.
            if ($("body").hasClass("fixed")) {
                $(".content-wrapper, .right-side").css('min-height', window_height - footer_height);
            } else {
                var postSetWidth;
                if (window_height >= sidebar_height) {
                    $(".content-wrapper, .right-side").css('min-height', window_height - neg);
                    postSetWidth = window_height - neg;
                } else {
                    $(".content-wrapper, .right-side").css('min-height', sidebar_height);
                    postSetWidth = sidebar_height;
                }

                //Fix for the control sidebar height
                var controlSidebar = $($.DEDUPProcess.options.controlSidebarOptions.selector);
                if (typeof controlSidebar !== "undefined") {
                    if (controlSidebar.height() > postSetWidth)
                        $(".content-wrapper, .right-side").css('min-height', controlSidebar.height());
                }

            }
        },
        fixSidebar: function () {
            //Make sure the body tag has the .fixed class
            if (!$("body").hasClass("fixed")) {
                if (typeof $.fn.slimScroll !== 'undefined') {
                    $(".sidebar").slimScroll({ destroy: true }).height("auto");
                }
                return;
            } else if (typeof $.fn.slimScroll === 'undefined' && window.console) {
                window.console.error("Error: the fixed layout requires the slimscroll plugin!");
            }
            //Enable slimscroll for fixed layout
            if ($.DEDUPProcess.options.sidebarSlimScroll) {
                if (typeof $.fn.slimScroll !== 'undefined') {
                    //Destroy if it exists
                    $(".sidebar").slimScroll({ destroy: true }).height("auto");
                    //Add slimscroll
                    $(".sidebar").slimScroll({
                        height: ($(window).height() - $(".main-header").height()) + "px",
                        color: "rgba(0,0,0,0.2)",
                        size: "3px"
                    });
                }
            }
        }
    };

    /* PushMenu()
     * ==========
     * Adds the push menu functionality to the sidebar.
     *
     * @type Function
     * @usage: $.DEDUPProcess.pushMenu("[data-toggle='offcanvas']")
     */
    $.DEDUPProcess.pushMenu = {
        activate: function (toggleBtn) {
            //Get the screen sizes
            var screenSizes = $.DEDUPProcess.options.screenSizes;

            //Enable sidebar toggle
            $(document).on('click', toggleBtn, function (e) {

                e.preventDefault();

                //Enable sidebar push menu
                if ($(window).width() > (screenSizes.sm - 1)) {
                    if ($("body").hasClass('sidebar-collapse')) {
                        $("body").removeClass('sidebar-collapse').trigger('expanded.pushMenu');
                    } else {
                        $("body").addClass('sidebar-collapse').trigger('collapsed.pushMenu');
                    }
                }
                    //Handle sidebar push menu for small screens
                else {
                    if ($("body").hasClass('sidebar-open')) {
                        $("body").removeClass('sidebar-open').removeClass('sidebar-collapse').trigger('collapsed.pushMenu');
                    } else {
                        $("body").addClass('sidebar-open').trigger('expanded.pushMenu');
                    }
                }
            });

            $(".content-wrapper").click(function () {
                //Enable hide menu when clicking on the content-wrapper on small screens
                if ($(window).width() <= (screenSizes.sm - 1) && $("body").hasClass("sidebar-open")) {
                    $("body").removeClass('sidebar-open');
                }
            });

            //Enable expand on hover for sidebar mini
            if ($.DEDUPProcess.options.sidebarExpandOnHover
              || ($('body').hasClass('fixed')
              && $('body').hasClass('sidebar-mini'))) {
                this.expandOnHover();
            }
        },
        expandOnHover: function () {
            var _this = this;
            var screenWidth = $.DEDUPProcess.options.screenSizes.sm - 1;
            //Expand sidebar on hover
            $('.main-sidebar').hover(function () {
                if ($('body').hasClass('sidebar-mini')
                  && $("body").hasClass('sidebar-collapse')
                  && $(window).width() > screenWidth) {
                    _this.expand();
                }
            }, function () {
                if ($('body').hasClass('sidebar-mini')
                  && $('body').hasClass('sidebar-expanded-on-hover')
                  && $(window).width() > screenWidth) {
                    _this.collapse();
                }
            });
        },
        expand: function () {
            $("body").removeClass('sidebar-collapse').addClass('sidebar-expanded-on-hover');
        },
        collapse: function () {
            if ($('body').hasClass('sidebar-expanded-on-hover')) {
                $('body').removeClass('sidebar-expanded-on-hover').addClass('sidebar-collapse');
            }
        }
    };

    /* Tree()
     * ======
     * Converts the sidebar into a multilevel
     * tree view menu.
     *
     * @type Function
     * @Usage: $.DEDUPProcess.tree('.sidebar')
     */
    $.DEDUPProcess.tree = function (menu) {
        var _this = this;
        var animationSpeed = $.DEDUPProcess.options.animationSpeed;
        $(document).off('click', menu + ' li a')
          .on('click', menu + ' li a', function (e) {
              //Get the clicked link and the next element
              var $this = $(this);
              var checkElement = $this.next();

              //Check if the next element is a menu and is visible
              if ((checkElement.is('.treeview-menu')) && (checkElement.is(':visible')) && (!$('body').hasClass('sidebar-collapse'))) {
                  //Close the menu
                  checkElement.slideUp(animationSpeed, function () {
                      checkElement.removeClass('menu-open');
                      //Fix the layout in case the sidebar stretches over the height of the window
                      //_this.layout.fix();
                  });
                  checkElement.parent("li").removeClass("active");
              }
                  //If the menu is not visible
              else if ((checkElement.is('.treeview-menu')) && (!checkElement.is(':visible'))) {
                  //Get the parent menu
                  var parent = $this.parents('ul').first();
                  //Close all open menus within the parent
                  var ul = parent.find('ul:visible').slideUp(animationSpeed);
                  //Remove the menu-open class from the parent
                  ul.removeClass('menu-open');
                  //Get the parent li
                  var parent_li = $this.parent("li");

                  //Open the target menu and add the menu-open class
                  checkElement.slideDown(animationSpeed, function () {
                      //Add the class active to the parent li
                      checkElement.addClass('menu-open');
                      parent.find('li.active').removeClass('active');
                      parent_li.addClass('active');
                      //Fix the layout in case the sidebar stretches over the height of the window
                      _this.layout.fix();
                  });
              }
              //if this isn't a link, prevent the page from being redirected
              if (checkElement.is('.treeview-menu')) {
                  e.preventDefault();
              }
          });
    };
}


/*
 * TODO LIST CUSTOM PLUGIN
 * -----------------------
 * This plugin depends on iCheck plugin for checkbox and radio inputs
 *
 * @type plugin
 * @usage $("#todo-widget").todolist( options );
 */
(function ($) {

    'use strict';

    $.fn.todolist = function (options) {
        // Render options
        var settings = $.extend({
            //When the user checks the input
            onCheck: function (ele) {
                return ele;
            },
            //When the user unchecks the input
            onUncheck: function (ele) {
                return ele;
            }
        }, options);

        return this.each(function () {

            if (typeof $.fn.iCheck !== 'undefined') {
                $('input', this).on('ifChecked', function () {
                    var ele = $(this).parents("li").first();
                    ele.toggleClass("done");
                    settings.onCheck.call(ele);
                });

                $('input', this).on('ifUnchecked', function () {
                    var ele = $(this).parents("li").first();
                    ele.toggleClass("done");
                    settings.onUncheck.call(ele);
                });
            } else {
                $('input', this).on('change', function () {
                    var ele = $(this).parents("li").first();
                    ele.toggleClass("done");
                    if ($('input', ele).is(":checked")) {
                        settings.onCheck.call(ele);
                    } else {
                        settings.onUncheck.call(ele);
                    }
                });
            }
        });
    };

    // NEW selector
    $.extend($.expr[":"], {
        "hrefEaquals": function (a, i, m) {
            if (jQuery(a).attr("href")) {
                return jQuery(a).attr("href").split("?")[0].toUpperCase() === m[3].toUpperCase();
            }
            return false;
        },
        "hrefContains": function (a, i, m) {
            if (jQuery(a).attr("href")) {
                return jQuery(a).attr("href").toUpperCase()
                    .indexOf(m[3].toUpperCase()) >= 0;
            }
            return false;
        }
    });
}(jQuery));
