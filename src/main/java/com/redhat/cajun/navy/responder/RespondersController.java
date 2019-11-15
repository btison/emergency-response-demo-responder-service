package com.redhat.cajun.navy.responder;

import java.util.List;
import java.util.Optional;

import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.model.ResponderStats;
import com.redhat.cajun.navy.responder.service.ResponderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class RespondersController {

    @Autowired
    private ResponderService responderService;

    @RequestMapping(value = "/stats", method = RequestMethod.GET, produces = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponderStats stats() {
        return responderService.getResponderStats();
    }

    @RequestMapping(value = "/responder/{id}", method = RequestMethod.GET, produces = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity<Responder> responder(@PathVariable long id) {
        Responder responder = responderService.getResponder(id);
        if (responder == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } else {
            return new ResponseEntity<>(responder, HttpStatus.OK);
        }
    }

    @RequestMapping(value = "/responder/byname/{name}", method = RequestMethod.GET, produces = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity<Responder> responderByName(@PathVariable String name) {
        Responder responder = responderService.getResponderByName(name);
        if (responder == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } else {
            return new ResponseEntity<>(responder, HttpStatus.OK);
        }
    }

    @RequestMapping(value = "/responders/available", method = RequestMethod.GET, produces = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Responder>> availableResponders(@RequestParam Optional<String> limit, @RequestParam Optional<String> offset) {
        List<Responder> responders;
        if (limit.isPresent()) {
            if (offset.isPresent()) {
                responders = responderService.availableResponders(Integer.parseInt(limit.get()), Integer.parseInt(offset.get()));
            } else {
                responders = responderService.availableResponders(Integer.parseInt(limit.get()),0);
            }
        } else {
            responders = responderService.availableResponders();
        }
        return new ResponseEntity<>(responders, HttpStatus.OK);
    }

    @RequestMapping(value = "/responders", method = RequestMethod.GET, produces = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Responder>> allResponders() {
        return new ResponseEntity<>(responderService.allResponders(), HttpStatus.OK);
    }

    @RequestMapping(value = "/responders/person", method = RequestMethod.GET, produces = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Responder>> personResponders() {
        return new ResponseEntity<>(responderService.personResponders(), HttpStatus.OK);
    }

    @RequestMapping(value = "/responder", method = RequestMethod.POST, consumes = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity createResponder(@RequestBody Responder responder) {

        responderService.createResponder(responder);
        return new ResponseEntity(HttpStatus.CREATED);
    }

    @RequestMapping(value = "/responders", method = RequestMethod.POST, consumes = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity createResponders(@RequestBody List<Responder> responders) {

        responderService.createResponders(responders);
        return new ResponseEntity(HttpStatus.CREATED);
    }

    @RequestMapping(value = "/responder", method = RequestMethod.PUT, consumes = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public ResponseEntity updateResponder(@RequestBody Responder responder) {
        responderService.updateResponder(responder);
        return new ResponseEntity(HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/responders/reset", method = RequestMethod.POST)
    public ResponseEntity reset() {
        responderService.reset();
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/responders/clear", method = RequestMethod.POST)
    public ResponseEntity clear() {
        responderService.clear();
        return new ResponseEntity(HttpStatus.OK);
    }
}
